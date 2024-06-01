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
        WITH gaps AS (
            SELECT
                COALESCE(LAG(id) OVER (ORDER BY id), 0) + 1 AS gap_start,
                id - 1 AS gap_end
            FROM (
                SELECT 0 AS id
                UNION ALL
                SELECT id FROM items
            ) t
        )
        SELECT gap_start AS range_start, gap_end AS range_end
        FROM gaps
        WHERE gap_end >= gap_start
        ",
    )
    .bind::<BigInt, _>(min_id)
    .bind::<BigInt, _>(max_id)
    .load::<MissingRange>(conn)
    .await?;

    Ok(res
        .iter()
        .map(|m| (m.range_start - 1, m.range_end + 1))
        .collect())
}
