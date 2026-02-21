pub mod migrations;
pub mod models;
pub mod schema;
#[cfg(any(test, feature = "sqlite-tests"))]
pub mod sqlite_test;
pub mod story_tree;

use diesel_async::{
    pg::AsyncPgConnection,
    pooled_connection::{
        deadpool::{BuildError, Pool},
        AsyncDieselConnectionManager,
    },
};

/// Builds the shared async Postgres connection pool used by ingest and read runtimes.
///
/// Callers should choose a pool size based on their own concurrency policy (for example
/// `min(workers, 64)`) instead of relying on crate-global defaults.
pub async fn build_db_pool(
    db_url: &str,
    max_size: usize,
) -> Result<Pool<AsyncPgConnection>, BuildError> {
    assert!(max_size > 0, "db pool max_size must be > 0");

    let pool_config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(db_url);
    let pool = Pool::builder(pool_config).max_size(max_size).build()?;

    Ok(pool)
}
