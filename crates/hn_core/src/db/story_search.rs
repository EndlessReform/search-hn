//! PostgreSQL search work persistence. Never hold a pooled connection during HTTP.
//! Source locks precede derived-row locks, matching the ingest trigger's lock order.
use crate::embeddings::Embedding;
use diesel::{
    sql_query,
    sql_types::{BigInt, Bool, Integer, Nullable, Text},
    QueryableByName,
};
use diesel_async::{pooled_connection::deadpool::Pool, AsyncPgConnection, RunQueryDsl};

pub type SearchPool = Pool<AsyncPgConnection>;
pub type DbResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, QueryableByName)]
pub struct PendingStory {
    #[diesel(sql_type = BigInt)]
    pub story_id: i64,
    #[diesel(sql_type = Text)]
    pub title: String,
    #[diesel(sql_type = Text)]
    pub url: String,
}

impl PendingStory {
    pub fn document(&self) -> String {
        format!("{}\n{}", self.title, self.url)
    }
}

#[derive(QueryableByName)]
struct Recipe {
    #[diesel(sql_type = Text)]
    recipe: String,
}

/// A missing table/comment fails explicitly; it cannot silently select a recipe.
pub async fn recipe(pool: &SearchPool) -> DbResult<String> {
    let mut conn = pool.get().await?;
    let row: Recipe =
        sql_query("SELECT obj_description('story_search'::regclass, 'pg_class') AS recipe")
            .get_result(&mut conn)
            .await?;
    if row.recipe.is_empty() {
        return Err("story_search has no embedding recipe".into());
    }
    Ok(row.recipe)
}

/// One loop per deployment: no claims, leases or database locks survive this read.
pub async fn due(
    pool: &SearchPool,
    batch_size: i64,
    start: i64,
    end: i64,
) -> DbResult<Vec<PendingStory>> {
    let mut conn = pool.get().await?;
    Ok(sql_query(
        "SELECT story_id, title, url FROM story_search
        WHERE embedding IS NULL AND retry_after <= now() AND story_id BETWEEN $1 AND $2
        ORDER BY retry_after, story_id LIMIT $3",
    )
    .bind::<BigInt, _>(start)
    .bind::<BigInt, _>(end)
    .bind::<BigInt, _>(batch_size)
    .load(&mut conn)
    .await?)
}

/// Lock the current source before touching its search row, and recheck both copies
/// of the text. A response can only UPDATE matching pending work; never INSERT it.
///
/// The same guard protects retry scheduling: an old failed request must not postpone
/// a newly edited document. PostgreSQL rechecks the locked source after a concurrent
/// update, and UPDATE rechecks the target tuple if the trigger changed it meanwhile.
pub async fn finish(
    pool: &SearchPool,
    story: &PendingStory,
    embedding: Option<&Embedding>,
    retry_seconds: i32,
) -> DbResult<bool> {
    let mut conn = pool.get().await?;
    let changed = sql_query(
        "WITH source AS MATERIALIZED (
            SELECT i.* FROM items i WHERE i.id = $1 FOR UPDATE
        ) UPDATE story_search s SET embedding = $4::halfvec(1024),
            retry_after = now() + make_interval(secs => $5)
        FROM source i WHERE s.story_id = i.id AND story_search_eligible(i::items)
            AND i.title = $2 AND coalesce(i.url, '') = $3
            AND s.title = $2 AND s.url = $3 AND s.embedding IS NULL",
    )
    .bind::<BigInt, _>(story.story_id)
    .bind::<Text, _>(&story.title)
    .bind::<Text, _>(&story.url)
    .bind::<Nullable<Text>, _>(embedding.map(Embedding::to_pg_text))
    .bind::<Integer, _>(retry_seconds)
    .execute(&mut conn)
    .await?;
    Ok(changed != 0)
}

#[derive(Debug, QueryableByName)]
pub struct Counts {
    #[diesel(sql_type = BigInt)]
    pub pending: i64,
    #[diesel(sql_type = BigInt)]
    pub due: i64,
}

/// Count durable unfinished work separately from work eligible to retry now.
/// Deferred invalid inputs therefore remain visible in progress reporting.
pub async fn counts(pool: &SearchPool, start: i64, end: i64) -> DbResult<Counts> {
    let mut conn = pool.get().await?;
    Ok(sql_query(
        "SELECT count(*) AS pending,
        count(*) FILTER (WHERE retry_after <= now()) AS due FROM story_search
        WHERE embedding IS NULL AND story_id BETWEEN $1 AND $2",
    )
    .bind::<BigInt, _>(start)
    .bind::<BigInt, _>(end)
    .get_result(&mut conn)
    .await?)
}

#[derive(QueryableByName)]
struct Seeded {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Bool, column_name = synced)]
    _synced: bool,
}

/// Walk a bounded ID range with keyset pagination. Lock in ID order and invoke the
/// same function as the trigger on current rows. Finished embeddings survive reruns.
/// Only a short source chunk is locked; no inference occurs in this statement.
pub async fn seed_chunk(
    pool: &SearchPool,
    after: i64,
    end: i64,
    limit: i64,
) -> DbResult<Option<i64>> {
    let mut conn = pool.get().await?;
    let rows: Vec<Seeded> = sql_query(
        "WITH source AS MATERIALIZED (
        SELECT i.* FROM items i WHERE id > $1 AND id <= $2 AND type = 'story'
        ORDER BY id LIMIT $3 FOR UPDATE
        ) SELECT id, CASE WHEN type = 'story' THEN sync_story_search(source::items) IS NULL
            ELSE false END AS synced FROM source ORDER BY id",
    )
    .bind::<BigInt, _>(after)
    .bind::<BigInt, _>(end)
    .bind::<BigInt, _>(limit)
    .load(&mut conn)
    .await?;
    // The selected volatile function has run on the server for each returned row;
    // its void-return marker is deliberately not interpreted as a changed-row count.
    Ok(rows.last().map(|r| r.id))
}
