pub mod monitoring;
use crate::state::AppState;
use prometheus_client::encoding::text::encode;

use axum::{extract::State, routing::get, Router};
use std::net::SocketAddr;
use std::sync::Arc;

// Health endpoint handler
async fn health_handler() -> String {
    "Healthy".to_string()
}

async fn expose_metrics(state: State<Arc<AppState>>) -> String {
    let mut buffer = String::new();
    let registry = state.registry.read().await;
    encode(&mut buffer, &registry).unwrap();
    buffer
}

pub async fn setup_server(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    let shutdown_token = state.shutdown_token.clone();
    let app = Router::new()
        .route("/", get(|| async { "Hello, world!" }))
        .route("/health", get(health_handler))
        .route("/metrics", get(expose_metrics))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let server_handle = tokio::spawn(async move {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(async {
                shutdown_token.cancelled().await;
            })
            .await
            .unwrap();
    });

    server_handle
}
