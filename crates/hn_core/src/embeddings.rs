//! Shared HTTP-only client for the homelab proxy. Model transformation belongs to
//! the server; this client validates its contract before any database write.
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::time::Duration;

pub const MODEL: &str = "pplx-embed-v1-0.6b";
pub const DIMENSIONS: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub enum Workload {
    Interactive,
    Bulk,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error("invalid embedding input: {0}")]
    InvalidInput(String),
    #[error("embedding transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("embedding endpoint HTTP {status}: {message}")]
    Http {
        status: u16,
        message: String,
        retry_after: Option<u64>,
    },
    #[error("embedding contract: {0}")]
    Contract(String),
    #[error("embedding recipe mismatch: expected {expected}, received {actual}")]
    Recipe { expected: String, actual: String },
}

/// Validated native coordinates. The private field prevents unvalidated writes.
#[derive(Debug, Clone, PartialEq)]
pub struct Embedding(Vec<i8>);

impl Embedding {
    /// Reject vectors that cannot be indexed with cosine or represented losslessly
    /// as the proxy's native signed coordinates. Halfvec conversion happens later.
    pub fn validate(values: Vec<f64>) -> Result<Self, EmbeddingError> {
        if values.len() != DIMENSIONS
            || values
                .iter()
                .any(|v| !v.is_finite() || v.fract() != 0.0 || !(-128.0..=127.0).contains(v))
            || values.iter().all(|v| *v == 0.0)
        {
            return Err(EmbeddingError::Contract(
                "expected 1024 nonzero-norm signed integer coordinates".into(),
            ));
        }
        Ok(Self(values.into_iter().map(|v| v as i8).collect()))
    }

    /// Bind as TEXT and cast to halfvec in SQL. Integers survive this representation
    /// exactly; no custom Diesel wire format or second quantization is required.
    pub fn to_pg_text(&self) -> String {
        serde_json::to_string(&self.0).expect("integer vector serializes")
    }
}

#[derive(Debug, Deserialize)]
struct Response {
    model: String,
    embedding_recipe: String,
    data: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
struct Entry {
    index: usize,
    embedding: Vec<f64>,
}

#[derive(Clone)]
pub struct EmbeddingClient {
    client: Client,
    endpoint: reqwest::Url,
    recipe: String,
}

impl EmbeddingClient {
    /// `recipe` comes from the search table's comment, not a second deployment setting.
    pub fn new(base_url: &str, recipe: String, timeout: Duration) -> Result<Self, EmbeddingError> {
        let endpoint =
            reqwest::Url::parse(&format!("{}/embeddings", base_url.trim_end_matches('/')))
                .map_err(|e| EmbeddingError::Contract(e.to_string()))?;
        if !matches!(endpoint.scheme(), "http" | "https") || recipe.is_empty() || timeout.is_zero()
        {
            return Err(EmbeddingError::Contract(
                "HTTP(S) URL, recipe and positive timeout required".into(),
            ));
        }
        Ok(Self {
            client: Client::builder().timeout(timeout).build()?,
            endpoint,
            recipe,
        })
    }

    /// One request, no hidden retries and no raw-route fallback. The caller owns
    /// durable retry scheduling and can split only input failures into smaller batches.
    pub async fn embed(
        &self,
        inputs: &[String],
        workload: Workload,
    ) -> Result<Vec<Embedding>, EmbeddingError> {
        if inputs.is_empty()
            || inputs.len() > 8
            || inputs.iter().any(|s| s.trim().is_empty() || s.len() > 8192)
            || inputs.iter().map(String::len).sum::<usize>() > 32768
        {
            return Err(EmbeddingError::InvalidInput(
                "1..8 texts, <=8192 bytes each and <=32768 bytes total required".into(),
            ));
        }
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(
                "X-Embedding-Workload",
                match workload {
                    Workload::Interactive => "interactive",
                    Workload::Bulk => "bulk",
                },
            )
            .json(&serde_json::json!({"model": MODEL, "input": inputs, "encoding_format": "float"}))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok());
            let body: serde_json::Value = response.json().await.unwrap_or(serde_json::Value::Null);
            let message = body
                .pointer("/error/message")
                .and_then(|v| v.as_str())
                .unwrap_or("no JSON error message")
                .to_owned();
            if matches!(
                status,
                StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE
            ) && matches!(
                body.pointer("/error/code").and_then(|v| v.as_str()),
                Some("invalid_request" | "backend_error")
            ) {
                return Err(EmbeddingError::InvalidInput(message));
            }
            return Err(EmbeddingError::Http {
                status: status.as_u16(),
                message,
                retry_after,
            });
        }
        let header_recipe = response
            .headers()
            .get("X-Embedding-Recipe")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body: Response = response
            .json()
            .await
            .map_err(|e| EmbeddingError::Contract(e.to_string()))?;
        if body.embedding_recipe != self.recipe
            || header_recipe.as_deref() != Some(self.recipe.as_str())
        {
            return Err(EmbeddingError::Recipe {
                expected: self.recipe.clone(),
                actual: format!("body={}, header={header_recipe:?}", body.embedding_recipe),
            });
        }
        if body.model != MODEL || body.data.len() != inputs.len() {
            return Err(EmbeddingError::Contract(
                "model or vector count mismatch".into(),
            ));
        }
        let mut ordered = vec![None; inputs.len()];
        for entry in body.data {
            if entry.index >= ordered.len() || ordered[entry.index].is_some() {
                return Err(EmbeddingError::Contract(
                    "duplicate or out-of-range response index".into(),
                ));
            }
            ordered[entry.index] = Some(Embedding::validate(entry.embedding)?);
        }
        Ok(ordered
            .into_iter()
            .map(|v| v.expect("unique indices cover inputs"))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinates_are_validated_before_storage() {
        for bad in [
            vec![0.; 1024],
            vec![1.; 1023],
            vec![0.5; 1024],
            vec![128.; 1024],
            vec![f64::NAN; 1024],
        ] {
            assert!(Embedding::validate(bad).is_err());
        }
        let mut valid = vec![-128.; 1024];
        valid[1] = 127.;
        let stored = Embedding::validate(valid).unwrap().to_pg_text();
        assert!(stored.starts_with("[-128,127,"));
    }
}
