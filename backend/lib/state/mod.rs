use diesel_async::{pg::AsyncPgConnection, pooled_connection::deadpool::Pool};
use prometheus_client::registry::Registry;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::server::monitoring::create_registry;

pub struct AppState {
    pub pool: Pool<AsyncPgConnection>,
    pub shutdown_token: CancellationToken,
    pub registry: RwLock<Registry>,
}

impl AppState {
    pub fn new(pool: Pool<AsyncPgConnection>, shutdown_token: CancellationToken) -> Self {
        Self {
            pool,
            shutdown_token,
            registry: RwLock::new(create_registry()),
        }
    }
}
