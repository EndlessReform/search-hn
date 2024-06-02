use diesel_async::{pg::AsyncPgConnection, pooled_connection::deadpool::Pool};
use tokio_util::sync::CancellationToken;

pub struct AppState {
    pub pool: Pool<AsyncPgConnection>,
    pub shutdown_token: CancellationToken,
}

impl AppState {
    pub fn new(pool: Pool<AsyncPgConnection>, shutdown_token: CancellationToken) -> Self {
        Self {
            pool,
            shutdown_token,
        }
    }
}
