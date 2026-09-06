//! Opt-in five-minute workload using an explicitly exported source slice.
//! This measures ingest overhead and progress, not retrieval quality or production capacity.
#[path = "support/search.rs"]
mod support;
use catchup_worker_lib::{
    embeddings::process_batch,
    sync_service::ingest_worker::{BatchPersister, IngestSource, PgBatchPersister},
};
use diesel::{prelude::*, sql_query, sql_types::Text};
use hn_core::{
    db::{models::Item, story_search as search},
    HnItem,
};
use serde_json::json;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "five-minute scratch load; requires TEST_SEARCH_SLICE binary COPY file"]
async fn mixed_ingestion_and_backfill() {
    let seconds = std::env::var("TEST_SEARCH_STRESS_SECONDS")
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap_or(150);
    assert!(seconds > 0);
    let mut phases = Vec::new();
    for enabled in [false, true] {
        phases.push(phase(enabled, seconds).await);
    }
    let output =
        serde_json::to_string_pretty(&json!({"seconds_per_phase":seconds,"phases":phases}))
            .unwrap();
    println!("{output}");
    if let Ok(path) = std::env::var("TEST_SEARCH_STRESS_REPORT") {
        std::fs::write(path, output).unwrap();
    }
}

async fn phase(enabled: bool, seconds: u64) -> serde_json::Value {
    let (db, pool) = database().await;
    execute(
        &db,
        "ALTER TABLE items DISABLE TRIGGER story_search_source_changed",
    );
    let slice = std::fs::File::open(
        std::env::var("TEST_SEARCH_SLICE").expect("explicit binary source slice required"),
    )
    .unwrap();
    let copied=tokio::process::Command::new("psql").args([&db.database_url(),"-X","-v","ON_ERROR_STOP=1","-c",
        "COPY items(id,deleted,type,\"by\",time,text,dead,parent,poll,url,score,title,parts,descendants) FROM STDIN WITH (FORMAT binary)"])
        .stdin(slice).output().await.unwrap();
    assert!(
        copied.status.success(),
        "{}",
        String::from_utf8_lossy(&copied.stderr)
    );
    if enabled {
        execute(
            &db,
            "ALTER TABLE items ENABLE TRIGGER story_search_source_changed",
        );
    }
    execute(&db, "ANALYZE items");
    let source_rows = count(&db, "SELECT count(*) n FROM items");
    let eligible = count(
        &db,
        "SELECT count(*) n FROM items i WHERE story_search_eligible(i)",
    );
    #[derive(QueryableByName)]
    struct Payload {
        #[diesel(sql_type=Text)]
        payload: String,
    }
    let raw: Vec<Payload> =
        sql_query("SELECT row_to_json(i)::text AS payload FROM items i ORDER BY id LIMIT 100000")
            .load(&mut PgConnection::establish(&db.database_url()).unwrap())
            .unwrap();
    let source: Vec<Item> = raw
        .into_iter()
        .map(|r| Item::from(serde_json::from_str::<HnItem>(&r.payload).unwrap()))
        .collect();
    assert!(
        source.len() >= 1000,
        "stress requires a useful source slice"
    );
    let (url, mock, server) = server(search::recipe(&pool).await.unwrap()).await;
    mock.mode.store(9, Ordering::SeqCst);
    let mut settings = args(url);
    settings.embedding_timeout_seconds = 40;
    let client = settings.client(&pool).await.unwrap();
    let start = Instant::now();
    let seed = tokio::spawn({
        let pool = pool.clone();
        async move {
            let mut cursor = 0;
            while let Some(last) = search::seed_chunk(&pool, cursor, i64::MAX, 1000)
                .await
                .unwrap()
            {
                cursor = last;
            }
        }
    });
    let embedding = tokio::spawn({
        let pool = pool.clone();
        async move {
            let mut saved = 0;
            while start.elapsed() < Duration::from_secs(seconds) {
                let rows = search::due(&pool, 4, 1, i64::MAX).await.unwrap();
                if rows.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else {
                    saved += process_batch(&pool, &client, rows, &settings)
                        .await
                        .unwrap()
                        .saved;
                }
            }
            saved
        }
    });
    let persister = PgBatchPersister::new(pool.clone());
    let mut timings = Vec::new();
    let mut offset = 0;
    let mut batches = 0;
    let mut max_pending = 0;
    let mut lock_samples = 0;
    let mut max_lock_waiters = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while start.elapsed() < Duration::from_secs(seconds) {
        tick.tick().await;
        let mut batch = (0..50)
            .map(|i| source[(offset + i) % source.len()].clone())
            .collect::<Vec<_>>();
        for (i, item) in batch.iter_mut().enumerate() {
            // Mostly ordinary metadata/replayed comment writes, with a smaller
            // share of text edits and threshold crossings to exercise invalidation.
            if item.type_.as_deref() == Some("story") {
                item.descendants = Some(batches);
                if i % 10 == 0 {
                    item.title = item
                        .title
                        .as_ref()
                        .map(|t| format!("{t} revision {batches}"));
                }
                if i % 15 == 0 {
                    item.score = Some(if batches % 2 == 0 { 24 } else { 25 });
                }
            }
        }
        let before = Instant::now();
        persister
            .persist_batch(&batch, &[], IngestSource::Realtime)
            .await
            .unwrap();
        timings.push(before.elapsed().as_secs_f64() * 1000.);
        batches += 1;
        offset = (offset + 50) % source.len();
        if batches % 25 == 0 {
            let counts = search::counts(&pool, 1, i64::MAX).await.unwrap();
            max_pending = max_pending.max(counts.pending);
            let waiting=count(&db,"SELECT count(*) n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'");
            lock_samples += 1;
            max_lock_waiters = max_lock_waiters.max(waiting);
            eprintln!(
                "trigger={enabled} elapsed={:.0}s batches={batches} pending={}",
                start.elapsed().as_secs_f64(),
                counts.pending
            );
        }
    }
    seed.await.unwrap();
    let saved = embedding.await.unwrap();
    let pending = search::counts(&pool, 1, i64::MAX).await.unwrap().pending;
    assert!(saved > 0, "embedding work must progress during ingestion");
    if enabled {
        assert_eq!(count(&db,"SELECT count(*) n FROM story_search s FULL JOIN items i ON i.id=s.story_id
            WHERE (s.story_id IS NOT NULL AND (NOT story_search_eligible(i) OR s.title IS DISTINCT FROM i.title OR s.url IS DISTINCT FROM coalesce(i.url,'')))
               OR (s.story_id IS NULL AND story_search_eligible(i))"),0,"source/search invariant after concurrent workload");
    }
    timings.sort_by(f64::total_cmp);
    let percentile = |p: f64| timings[((timings.len() - 1) as f64 * p) as usize];
    let result = json!({"trigger_enabled":enabled,"source_rows":source_rows,"initial_eligible":eligible,
        "ingest_rows":batches*50,"elapsed_seconds":start.elapsed().as_secs_f64(),
        "batch_ms":{"p50":percentile(0.5),"p95":percentile(0.95),"p99":percentile(0.99)},
        "embeddings_saved":saved,"pending_at_end":pending,"max_sampled_pending":max_pending,
        "lock_samples":lock_samples,"max_sampled_lock_waiters":max_lock_waiters,
        "database_deadlocks":count(&db,"SELECT deadlocks n FROM pg_stat_database WHERE datname=current_database()"),
        "search_bytes":count(&db,"SELECT pg_total_relation_size('story_search') n")});
    server.abort();
    result
}
