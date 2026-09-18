//! Shared embedding service: validate text, bound admission, forward native priority,
//! and convert pooled floats into the pinned Pplx representation.
//!
//! There is no durable work here. Callers retain their unfinished documents and retry
//! overloads later. Separate permits reserve interactive capacity even during bulk work.
mod config;
pub mod contract;
mod docs;
mod error;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
pub use config::Config;
use contract::{BackendResponse, MODEL, RECIPE, Request, transform};
use error::ApiError;
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct Service {
    client: reqwest::Client,
    backend: String,
    interactive: Arc<Semaphore>,
    bulk: Arc<Semaphore>,
}

/// Build a standalone router. Invalid connection settings fail at startup.
/// No queued Rust tasks accumulate: full admission immediately returns HTTP 429.
pub fn app(config: Config) -> Result<Router, Box<dyn std::error::Error>> {
    let url = reqwest::Url::parse(&config.backend)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("EMBED_BACKEND must be an HTTP(S) origin without a path/query".into());
    }
    let state = Service {
        client: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(config.timeout_seconds))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        backend: config.backend.trim_end_matches('/').into(),
        interactive: Arc::new(Semaphore::new(config.interactive_limit as usize)),
        bulk: Arc::new(Semaphore::new(1)),
    };
    Ok(Router::new()
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/models", get(models))
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(ready))
        .route("/usage.md", get(docs::usage))
        .route("/llms.txt", get(docs::llms))
        .layer(DefaultBodyLimit::max(256 * 1024))
        .with_state(state))
}

async fn models() -> Json<serde_json::Value> {
    Json(
        json!({"object":"list", "data":[{"id":MODEL, "object":"model", "created":0,
        "owned_by":"perplexity", "embedding_recipe":RECIPE}]}),
    )
}

/// Liveness is local; readiness checks that the upstream engine is serving.
async fn ready(State(state): State<Service>) -> Result<&'static str, ApiError> {
    let response = state
        .client
        .get(format!("{}/health", state.backend))
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(ApiError::transport)?;
    if !response.status().is_success() {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "backend_not_ready",
            "vLLM is not ready",
        ));
    }
    Ok("ready\n")
}

/// The permit lives through response validation and drops on every return/cancellation.
/// vLLM (priority policy) selects waiting priority 0 before 1; active GPU work is not preempted.
async fn embeddings(
    State(state): State<Service>,
    headers: HeaderMap,
    request: Result<Json<Request>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let priority = match headers.get("x-embedding-workload").map(|v| v.to_str()) {
        None | Some(Ok("interactive")) => 0,
        Some(Ok("bulk")) => 1,
        _ => {
            return Err(ApiError::bad_input(
                "X-Embedding-Workload must be interactive or bulk",
            ));
        }
    };
    let Json(request) =
        request.map_err(|e| ApiError::new(e.status(), "invalid_request", e.body_text()))?;
    let input = request.validate().map_err(ApiError::bad_input)?;
    let semaphore = if priority == 0 {
        &state.interactive
    } else {
        &state.bulk
    };
    let _permit = semaphore.try_acquire().map_err(|_| {
        ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            "workload capacity is full; retry later with backoff",
        )
    })?;
    let started = Instant::now();
    let count = input.len();
    let result = forward(&state, input, priority).await;
    tracing::info!(
        priority,
        count,
        elapsed_ms = started.elapsed().as_millis(),
        success = result.is_ok(),
        "embedding request finished"
    );
    Ok(([("x-embedding-recipe", RECIPE)], Json(result?)))
}

/// One upstream attempt only; retries belong to the caller, which knows its deadline.
async fn forward(
    state: &Service,
    input: Vec<String>,
    priority: i32,
) -> Result<contract::Response, ApiError> {
    let count = input.len();
    let response = state
        .client
        .post(format!("{}/v1/embeddings", state.backend))
        .json(
            &json!({"model":MODEL, "input":input, "encoding_format":"float", "priority":priority}),
        )
        .send()
        .await
        .map_err(ApiError::transport)?;
    let status = response.status();
    if !status.is_success() {
        // Preserve useful token-limit errors, while mapping backend availability failures.
        let bytes = limited_body(response, 64 * 1024).await?;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        let message = body
            .pointer("/error/message")
            .or_else(|| body.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("vLLM rejected the request");
        let code = if matches!(
            status,
            StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE | StatusCode::TOO_MANY_REQUESTS
        ) {
            status
        } else {
            StatusCode::BAD_GATEWAY
        };
        return Err(ApiError::new(code, "backend_error", message));
    }
    let bytes = limited_body(response, 2 * 1024 * 1024).await?;
    let raw: BackendResponse = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::backend("invalid JSON embedding response from vLLM"))?;
    transform(raw, count).map_err(ApiError::backend)
}

/// Bound backend response allocation too, including responses without Content-Length.
async fn limited_body(mut response: reqwest::Response, max: usize) -> Result<Vec<u8>, ApiError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(ApiError::transport)? {
        if bytes.len() + chunk.len() > max {
            return Err(ApiError::backend("vLLM response exceeds size limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;
