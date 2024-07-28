use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::BigInt;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

#[derive(QueryableByName, Debug)]
#[diesel(table_name = items)] // Specify the table name if needed for context
struct MissingRange {
    #[diesel(sql_type = BigInt)]
    range_start: i64,
    #[diesel(sql_type = BigInt)]
    range_end: i64,
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
