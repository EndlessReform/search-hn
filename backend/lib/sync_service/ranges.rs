use diesel::dsl::{max, min};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::BigInt;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::db::schema::items;

#[derive(QueryableByName, Debug)]
#[table_name = "items"] // Specify the table name if needed for context
struct MissingRange {
    #[sql_type = "BigInt"]
    range_start: i64,
    #[sql_type = "BigInt"]
    range_end: i64,
}

/// Gets min and max IDs on DB
async fn get_min_max_ids(
    conn: &mut AsyncPgConnection,
) -> Result<(i64, i64), diesel::result::Error> {
    let res: (Option<i64>, Option<i64>) = items::dsl::items
        .select((min(items::dsl::id), max(items::dsl::id)))
        .first(conn)
        .await?;

    match res {
        (Some(min_id), Some(max_id)) => Ok((min_id, max_id)),
        _ => Err(diesel::result::Error::NotFound),
    }
}

pub async fn get_missing_ranges(
    conn: &mut AsyncPgConnection,
    min_id: i64,
    max_id: i64,
) -> Result<Vec<(i64, i64)>, diesel::result::Error> {
    let res: Vec<MissingRange> = sql_query(
        "
        WITH missing_ids AS (
            SELECT gs.id 
            FROM generate_series($1, $2) AS gs(id)
            LEFT JOIN items ON gs.id = items.id
            WHERE items.id IS NULL
        )
        SELECT 
            MIN(id) AS range_start,
            MAX(id) AS range_end
        FROM (
            SELECT 
                id,
                id - LAG(id, 1, id) OVER (ORDER BY id) AS diff
            FROM missing_ids
        ) sub
        GROUP BY diff
        ",
    )
    .bind::<BigInt, _>(min_id)
    .bind::<BigInt, _>(max_id)
    .load::<MissingRange>(conn)
    .await?;

    Ok(res.iter().map(|m| (m.range_start, m.range_end)).collect())
}
