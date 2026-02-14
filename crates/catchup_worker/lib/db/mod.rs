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

pub async fn build_db_pool(db_url: &str) -> Result<Pool<AsyncPgConnection>, BuildError> {
    // TODO: I should probably move this but the type is a bit weird
    let pool_config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(db_url);
    let pool = Pool::builder(pool_config).build()?;

    Ok(pool)
}
