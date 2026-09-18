#[path = "support/search.rs"]
mod support;
use hn_core::{db::story_search as search, embeddings::Embedding};
use support::*;

#[tokio::test]
async fn migration_down_and_reapply_preserve_source_and_old_fts() {
    use diesel::{Connection, PgConnection};
    use diesel_migrations::MigrationHarness;
    let (db, _pool) = database().await;
    insert(&db, 1);
    let mut conn = PgConnection::establish(&db.database_url()).unwrap();
    conn.revert_last_migration(hn_core::db::migrations::postgres_migrations())
        .unwrap();
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM items WHERE search_tsv IS NOT NULL"
        ),
        1
    );
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM pg_extension WHERE extname IN ('vector','pg_textsearch')"
        ),
        2
    );
    hn_core::db::migrations::run_postgres_migrations(&mut conn);
    assert_eq!(
        count(&db, "SELECT count(*) n FROM story_search"),
        0,
        "migration does not backfill source"
    );
    execute(&db, "UPDATE items SET score=26 WHERE id=1");
    assert_eq!(count(&db, "SELECT count(*) n FROM story_search"), 1);
}

#[tokio::test]
async fn admission_edits_tombstones_and_rollback() {
    let (db, pool) = database().await;
    insert(&db, 2);
    let vector = Embedding::validate(vec![1.; 1024]).unwrap();
    let rows = search::due(&pool, 8, 1, 100).await.unwrap();
    for row in &rows {
        assert!(search::finish(&pool, row, Some(&vector), 0).await.unwrap());
    }
    execute(&db, "UPDATE items SET score=30, descendants=17 WHERE id=1;
        UPDATE items SET title=title,url=url,score=score WHERE id=1;
        UPDATE items SET story_id=1 WHERE id=1;
        INSERT INTO items(id,type,title,score,time) VALUES(3,'comment','database comment',50,1700000000),
            (4,'story','below',24,1700000000),(5,'story','   ',50,1700000000),
            (6,'story','bad time',50,0),(7,'story','missing time',50,NULL)");
    assert_eq!(count(&db, "SELECT count(*) n FROM story_search"), 2);
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM story_search WHERE embedding IS NOT NULL"
        ),
        2
    );
    execute(
        &db,
        "UPDATE items SET score=25 WHERE id=4; UPDATE items SET title='edited' WHERE id=1",
    );
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM story_search WHERE embedding IS NULL"
        ),
        2
    );
    assert!(!search::finish(&pool, &rows[0], Some(&vector), 0)
        .await
        .unwrap());
    execute(
        &db,
        "BEGIN; UPDATE items SET deleted=true WHERE id=2; ROLLBACK;",
    );
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM story_search WHERE story_id=2 AND embedding IS NOT NULL"
        ),
        1
    );
    execute(&db,"UPDATE items SET type=NULL, deleted=true,title=NULL,url=NULL,score=NULL,time=NULL WHERE id=2;
        UPDATE items SET score=24 WHERE id=4; DELETE FROM items WHERE id=1");
    assert_eq!(count(&db, "SELECT count(*) n FROM story_search"), 0);
    execute(&db, "UPDATE items SET score=25 WHERE id=4");
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM story_search WHERE embedding IS NULL"
        ),
        1
    );
    execute(&db, "UPDATE items SET dead=true WHERE id=4");
    assert_eq!(count(&db, "SELECT count(*) n FROM story_search"), 0);
}

#[tokio::test]
async fn backfill_preserves_finished_vectors_and_retries_and_indexes_work() {
    let (db, pool) = database().await;
    execute(
        &db,
        "ALTER TABLE items DISABLE TRIGGER story_search_source_changed",
    );
    insert(&db, 10);
    execute(
        &db,
        "ALTER TABLE items ENABLE TRIGGER story_search_source_changed",
    );
    let mut cursor = 0;
    while let Some(last) = search::seed_chunk(&pool, cursor, 10, 3).await.unwrap() {
        cursor = last;
    }
    assert_eq!(count(&db, "SELECT count(*) n FROM story_search"), 10);
    let rows = search::due(&pool, 8, 1, 10).await.unwrap();
    let vector = Embedding::validate(vec![1.; 1024]).unwrap();
    search::finish(&pool, &rows[0], Some(&vector), 0)
        .await
        .unwrap();
    search::finish(&pool, &rows[1], None, 3600).await.unwrap();
    search::seed_chunk(&pool, 0, 10, 100).await.unwrap();
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM story_search WHERE embedding IS NOT NULL"
        ),
        1
    );
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM story_search WHERE retry_after>now()"
        ),
        1
    );
    // Actual index lookups, not simply checking that CREATE INDEX succeeded.
    execute(&db,&format!("SET enable_seqscan=off; SELECT story_id FROM story_search ORDER BY embedding <=> '{}'::halfvec(1024) LIMIT 1; SELECT story_id FROM story_search ORDER BY title <@> to_bm25query('database','story_search_title_bm25') LIMIT 3",vector.to_pg_text()));
}
