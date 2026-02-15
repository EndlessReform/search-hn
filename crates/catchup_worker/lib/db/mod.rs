pub mod models;
pub mod schema;
#[cfg(any(test, feature = "sqlite-tests"))]
pub mod sqlite_test;

use diesel_async::{
    pg::AsyncPgConnection,
    pooled_connection::{
        deadpool::{BuildError, Pool},
        AsyncDieselConnectionManager,
    },
};

/// Builds the shared async Postgres connection pool used by the worker runtime.
///
/// Callers are expected to choose a pool size based on their ingest concurrency model
/// (for example `min(workers, 64)`), rather than relying on crate defaults.
pub async fn build_db_pool(
    db_url: &str,
    max_size: usize,
) -> Result<Pool<AsyncPgConnection>, BuildError> {
    assert!(max_size > 0, "db pool max_size must be > 0");

    // TODO: I should probably move this but the type is a bit weird
    let pool_config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(db_url);
    let pool = Pool::builder(pool_config).max_size(max_size).build()?;

    Ok(pool)
}
