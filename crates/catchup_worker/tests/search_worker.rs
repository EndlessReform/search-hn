#[path = "support/search.rs"]
mod support;
use catchup_worker_lib::embeddings::{process_batch, supervise};
use hn_core::{db::story_search as search, embeddings::EmbeddingClient};
use std::{sync::atomic::Ordering, time::Duration};
use support::*;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn edits_deletes_demotions_and_abort_during_inference() {
    let (db, pool) = database().await;
    insert(&db, 4);
    let (url, mock, server) = server(search::recipe(&pool).await.unwrap()).await;
    let args = args(url);
    let client = args.client(&pool).await.unwrap();
    mock.mode.store(1, Ordering::SeqCst);
    let rows = search::due(&pool, 4, 1, 10).await.unwrap();
    let task = tokio::spawn({
        let pool = pool.clone();
        let args = args.clone();
        let client = client.clone();
        async move { process_batch(&pool, &client, rows, &args).await }
    });
    tokio::time::timeout(Duration::from_secs(5), mock.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    execute(&db,"UPDATE items SET title='new title',url=NULL WHERE id=1; DELETE FROM items WHERE id=2; UPDATE items SET score=24 WHERE id=3;");
    mock.release.add_permits(1);
    let report = task.await.unwrap().unwrap();
    assert_eq!((report.saved, report.discarded), (1, 3));
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM story_search WHERE embedding IS NOT NULL"
        ),
        1
    );
    // Abort the loop while a request is pending. A new loop finds the same durable row.
    let cancel = CancellationToken::new();
    let handle = supervise(pool.clone(), args.clone(), cancel.clone());
    tokio::time::timeout(Duration::from_secs(5), mock.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    cancel.cancel();
    handle.await.unwrap();
    assert_eq!(search::counts(&pool, 1, 10).await.unwrap().pending, 1);
    mock.mode.store(0, Ordering::SeqCst);
    mock.release.add_permits(1);
    let rows = search::due(&pool, 4, 1, 10).await.unwrap();
    assert_eq!(
        process_batch(&pool, &client, rows, &args)
            .await
            .unwrap()
            .saved,
        1
    );
    server.abort();
}

#[tokio::test]
async fn poison_is_isolated_but_outage_and_contract_errors_do_not_split() {
    let (db, pool) = database().await;
    insert(&db, 4);
    execute(&db, "UPDATE items SET title='poison' WHERE id=2");
    let (url, mock, server) = server(search::recipe(&pool).await.unwrap()).await;
    let args = args(url);
    let client = args.client(&pool).await.unwrap();
    mock.mode.store(3, Ordering::SeqCst);
    let report = process_batch(
        &pool,
        &client,
        search::due(&pool, 4, 1, 10).await.unwrap(),
        &args,
    )
    .await
    .unwrap();
    assert_eq!((report.saved, report.delayed), (3, 1));
    assert_eq!(mock.calls.load(Ordering::SeqCst), 5);
    for mode in [2, 4, 5, 6, 7, 8] {
        execute(
            &db,
            "UPDATE story_search SET embedding=NULL,retry_after=now()",
        );
        let calls = mock.calls.load(Ordering::SeqCst);
        mock.mode.store(mode, Ordering::SeqCst);
        assert!(process_batch(
            &pool,
            &client,
            search::due(&pool, 4, 1, 10).await.unwrap(),
            &args
        )
        .await
        .is_err());
        assert_eq!(
            mock.calls.load(Ordering::SeqCst) - calls,
            1,
            "mode {mode} must not split"
        );
        assert_eq!(
            count(
                &db,
                "SELECT count(*) n FROM story_search WHERE embedding IS NOT NULL"
            ),
            0
        );
        assert_eq!(search::counts(&pool, 1, 10).await.unwrap().due, 0);
    }
    // Successful entries arrive reversed; client maps them back by index.
    mock.mode.store(0, Ordering::SeqCst);
    let values = client
        .embed(
            &["first".into(), "second".into()],
            hn_core::embeddings::Workload::Bulk,
        )
        .await
        .unwrap();
    assert!(values[0].to_pg_text().starts_with("[1,1,"));
    assert!(values[1].to_pg_text().starts_with("[2,2,"));
    server.abort();
}

#[tokio::test]
async fn source_lock_rechecks_after_waiting_for_ingestion() {
    use diesel::{connection::SimpleConnection, Connection, PgConnection};
    let (db, pool) = database().await;
    insert(&db, 1);
    let row = search::due(&pool, 1, 1, 10).await.unwrap().remove(0);
    let mut editing = PgConnection::establish(&db.database_url()).unwrap();
    editing
        .batch_execute("BEGIN; UPDATE items SET title='uncommitted edit' WHERE id=1")
        .unwrap();
    let finish = tokio::spawn({
        let pool = pool.clone();
        async move {
            search::finish(
                &pool,
                &row,
                Some(&hn_core::embeddings::Embedding::validate(vec![1.; 1024]).unwrap()),
                0,
            )
            .await
        }
    });
    // Observe an actual lock wait before committing, rather than racing a sleep.
    tokio::time::timeout(Duration::from_secs(5),async {
        while count(&db,"SELECT count(*) n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'")==0 {tokio::task::yield_now().await;}
    }).await.unwrap();
    editing.batch_execute("COMMIT").unwrap();
    assert!(!finish.await.unwrap().unwrap());
    assert_eq!(count(&db,"SELECT count(*) n FROM story_search WHERE title='uncommitted edit' AND embedding IS NULL"),1);
}

#[tokio::test]
async fn supervisor_recovers_from_endpoint_failure() {
    let (db, pool) = database().await;
    insert(&db, 1);
    let (url, mock, server) = server(search::recipe(&pool).await.unwrap()).await;
    let args = args(url);
    mock.mode.store(2, Ordering::SeqCst);
    let cancel = CancellationToken::new();
    let handle = supervise(pool.clone(), args, cancel.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        while mock.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    execute(&db,"UPDATE items SET descendants=10 WHERE id=1; INSERT INTO items(id,type,parent) VALUES(2,'comment',1)");
    mock.mode.store(0, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), async {
        while search::counts(&pool, 1, 10).await.unwrap().pending > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    handle.await.unwrap();
    server.abort();
}

#[tokio::test]
#[ignore = "opt-in shared inference smoke; requires TEST_EMBEDDING_BASE_URL"]
async fn live_proxy_smoke() {
    // Opt-in separately from deterministic tests; uses two documents and no writes
    // outside its fresh scratch database.
    let url = std::env::var("TEST_EMBEDDING_BASE_URL").expect("explicit shared proxy URL required");
    let (db, pool) = database().await;
    insert(&db, 2);
    let args = args(url.clone());
    let client = EmbeddingClient::new(
        &url,
        search::recipe(&pool).await.unwrap(),
        Duration::from_secs(40),
    )
    .unwrap();
    let report = process_batch(
        &pool,
        &client,
        search::due(&pool, 2, 1, 10).await.unwrap(),
        &args,
    )
    .await
    .unwrap();
    assert_eq!(report.saved, 2);
}

#[tokio::test]
async fn backfill_command_reruns_without_reembedding() {
    let (db, pool) = database().await;
    execute(
        &db,
        "ALTER TABLE items DISABLE TRIGGER story_search_source_changed",
    );
    insert(&db, 4);
    execute(
        &db,
        "ALTER TABLE items ENABLE TRIGGER story_search_source_changed",
    );
    let (url, mock, server) = server(search::recipe(&pool).await.unwrap()).await;
    for _ in 0..2 {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_catchup_worker"))
            .args([
                "embedding-backfill",
                "--database-url",
                &db.database_url(),
                "--embedding-base-url",
                &url,
                "--start-id",
                "1",
                "--end-id",
                "4",
                "--source-chunk-size",
                "2",
            ])
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(mock.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        count(
            &db,
            "SELECT count(*) n FROM story_search WHERE embedding IS NOT NULL"
        ),
        4
    );
    server.abort();
}
