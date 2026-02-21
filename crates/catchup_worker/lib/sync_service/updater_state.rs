use diesel::sql_types::{BigInt, Nullable};
use diesel::{sql_query, QueryableByName};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::RunQueryDsl;

use super::error::Error;

#[derive(Debug, QueryableByName)]
struct EpochRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    epoch_seconds: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct IdRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

/// Reads persisted updater heartbeat timestamp (unix seconds), if present.
pub async fn load_last_sse_event_epoch(
    pool: &Pool<diesel_async::AsyncPgConnection>,
) -> Result<Option<i64>, Error> {
    let mut conn = pool.get().await?;
    let mut rows = sql_query(
        "SELECT EXTRACT(EPOCH FROM last_sse_event_at)::BIGINT AS epoch_seconds FROM updater_state WHERE id = 1",
    )
    .load::<EpochRow>(&mut conn)
    .await?;

    Ok(rows.pop().and_then(|row| row.epoch_seconds))
}

/// Upserts the persisted updater heartbeat timestamp.
pub async fn save_last_sse_event_epoch(
    pool: &Pool<diesel_async::AsyncPgConnection>,
    epoch_seconds: i64,
) -> Result<(), Error> {
    let mut conn = pool.get().await?;
    sql_query(format!(
        "INSERT INTO updater_state (id, last_sse_event_at, updated_at) \
         VALUES (1, to_timestamp({epoch_seconds}), NOW()) \
         ON CONFLICT (id) DO UPDATE \
         SET last_sse_event_at = EXCLUDED.last_sse_event_at, updated_at = NOW()"
    ))
    .execute(&mut conn)
    .await?;
    Ok(())
}

/// Finds the earliest known item ID whose creation timestamp is within the selected replay
/// window.
pub async fn find_rescan_start_id_from_time(
    pool: &Pool<diesel_async::AsyncPgConnection>,
    from_epoch_seconds: i64,
) -> Result<i64, Error> {
    let mut conn = pool.get().await?;
    let mut rows = sql_query(format!(
        "SELECT COALESCE(MIN(id), 1) AS id FROM items WHERE time IS NOT NULL AND time >= {from_epoch_seconds}"
    ))
    .load::<IdRow>(&mut conn)
    .await?;

    Ok(rows.pop().map_or(1, |row| row.id.max(1)))
}

/// Returns the highest mirrored item ID currently present in `items`.
pub async fn load_max_item_id(
    pool: &Pool<diesel_async::AsyncPgConnection>,
) -> Result<Option<i64>, Error> {
    let mut conn = pool.get().await?;
    let mut rows = sql_query("SELECT COALESCE(MAX(id), 0) AS id FROM items")
        .load::<IdRow>(&mut conn)
        .await?;
    let max_id = rows.pop().map_or(0, |row| row.id);
    if max_id <= 0 {
        Ok(None)
    } else {
        Ok(Some(max_id))
    }
}
