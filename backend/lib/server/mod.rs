use axum::{routing::get, Router};
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;

// Health endpoint handler
async fn health_handler() -> String {
    "Healthy".to_string()
}

pub async fn setup_server(shutdown_token: CancellationToken) -> tokio::task::JoinHandle<()> {
    let app = Router::new()
        .route("/", get(|| async { "Hello, world!" }))
        .route("/health", get(health_handler));

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
