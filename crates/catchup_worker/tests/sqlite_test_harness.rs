#![cfg(feature = "sqlite-tests")]

use catchup_worker_lib::db::sqlite_test::setup_in_memory_sqlite;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Text};

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct NameRow {
    #[diesel(sql_type = Text)]
    name: String,
}

#[derive(QueryableByName)]
struct SegmentIdRow {
    #[diesel(sql_type = BigInt)]
    segment_id: i64,
}

#[test]
fn sqlite_harness_runs_expected_schema_migrations() {
    let mut conn = setup_in_memory_sqlite();

    let rows: Vec<NameRow> = sql_query(
        "
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name IN ('items', 'kids', 'users', 'ingest_segments', 'ingest_exceptions')
        ORDER BY name
        ",
    )
    .load(&mut conn)
    .expect("failed to query sqlite_master");

    let names: Vec<String> = rows.into_iter().map(|row| row.name).collect();
    assert_eq!(
        names,
        vec![
            "ingest_exceptions".to_string(),
            "ingest_segments".to_string(),
            "items".to_string(),
            "kids".to_string(),
            "users".to_string(),
        ]
    );
}

#[test]
fn sqlite_harness_creates_ordered_kids_lookup_index() {
    let mut conn = setup_in_memory_sqlite();

    let index_count: CountRow = sql_query(
        "
        SELECT COUNT(*) AS count
        FROM sqlite_master
        WHERE type = 'index'
          AND name = 'idx_kids_item_order'
        ",
    )
    .get_result(&mut conn)
    .expect("failed to query sqlite index metadata");

    assert_eq!(
        index_count.count, 1,
        "expected idx_kids_item_order index to exist"
    );
}

#[test]
fn sqlite_harness_enforces_ingest_segment_checks() {
    let mut conn = setup_in_memory_sqlite();

    let bad_status = sql_query(
        "
        INSERT INTO ingest_segments (start_id, end_id, status)
        VALUES (1, 10, 'not_real')
        ",
    )
    .execute(&mut conn)
    .expect_err("expected status check constraint to fail");
    assert!(
        bad_status.to_string().contains("CHECK constraint failed"),
        "unexpected sqlite error: {bad_status}"
    );

    let bad_range = sql_query(
        "
        INSERT INTO ingest_segments (start_id, end_id, status)
        VALUES (10, 1, 'pending')
        ",
    )
    .execute(&mut conn)
    .expect_err("expected range check constraint to fail");
    assert!(
        bad_range.to_string().contains("CHECK constraint failed"),
        "unexpected sqlite error: {bad_range}"
    );
}

#[test]
fn sqlite_harness_enforces_exception_fk_cascade() {
    let mut conn = setup_in_memory_sqlite();

    sql_query(
        "
        INSERT INTO ingest_segments (start_id, end_id, status)
        VALUES (100, 200, 'pending')
        ",
    )
    .execute(&mut conn)
    .expect("failed to insert ingest segment");

    let segment_id: SegmentIdRow = sql_query(
        "
        SELECT segment_id
        FROM ingest_segments
        LIMIT 1
        ",
    )
    .get_result(&mut conn)
    .expect("failed to read segment_id");

    sql_query(
        "
        INSERT INTO ingest_exceptions (segment_id, item_id, state)
        VALUES (?, 150, 'pending')
        ",
    )
    .bind::<BigInt, _>(segment_id.segment_id)
    .execute(&mut conn)
    .expect("failed to insert ingest exception");

    sql_query(
        "
        DELETE FROM ingest_segments
        WHERE segment_id = ?
        ",
    )
    .bind::<BigInt, _>(segment_id.segment_id)
    .execute(&mut conn)
    .expect("failed to delete ingest segment");

    let remaining: CountRow = sql_query(
        "
        SELECT COUNT(*) AS count
        FROM ingest_exceptions
        ",
    )
    .get_result(&mut conn)
    .expect("failed to count exceptions");

    assert_eq!(remaining.count, 0);
}
