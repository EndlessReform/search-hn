pub mod monitoring;
use crate::state::AppState;
use prometheus_client::encoding::text::encode;

use axum::{extract::State, routing::get, Router};
use monitoring::{CATCHUP_METRICS, REALTIME_METRICS};
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
    setup_server_with_addr(state, SocketAddr::from(([0, 0, 0, 0], 3000)))
        .await
        .expect("failed to bind metrics server")
}

/// Starts the health/metrics HTTP server on the supplied socket address.
pub async fn setup_server_with_addr(
    state: Arc<AppState>,
    addr: SocketAddr,
) -> Result<tokio::task::JoinHandle<()>, std::io::Error> {
    {
        let mut registry = state.registry.write().await;

        CATCHUP_METRICS
            .get_or_init(|| async {
                monitoring::CatchupMetrics::register(&mut registry, "catchup")
            })
            .await;

        REALTIME_METRICS
            .get_or_init(|| async {
                monitoring::RealtimeMetrics::register(&mut registry, "realtime")
            })
            .await;

        monitoring::register_build_info_metric(&mut registry, "worker");
    }

    let shutdown_token = state.shutdown_token.clone();
    let app = Router::new()
        .route("/", get(|| async { "Hello, world!" }))
        .route("/health", get(health_handler))
        .route("/metrics", get(expose_metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let server_handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            shutdown_token.cancelled().await;
        })
        .await
        .unwrap();
    });

    Ok(server_handle)
}
