use diesel_async::{pg::AsyncPgConnection, pooled_connection::deadpool::Pool};
use prometheus_client::registry::Registry;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

pub struct AppState {
    pub pool: Pool<AsyncPgConnection>,
    pub shutdown_token: CancellationToken,
    pub registry: RwLock<Registry>,
    /// Readiness of the realtime Firebase stream, separate from process liveness.
    pub realtime_healthy: Arc<AtomicBool>,
}

impl AppState {
    pub fn new(pool: Pool<AsyncPgConnection>, shutdown_token: CancellationToken) -> Self {
        Self {
            pool,
            shutdown_token,
            registry: RwLock::new(<Registry>::default()),
            realtime_healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.realtime_healthy.load(Ordering::Relaxed)
    }
}
