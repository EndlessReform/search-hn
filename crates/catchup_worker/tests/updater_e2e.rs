use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::sse::{Event, Sse},
    routing::get,
    Json, Router,
};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Nullable};
use futures_util::{
    stream::{self, BoxStream},
    StreamExt,
};
use hn_core::db::migrations::run_postgres_migrations;
use serde_json::Value;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

#[derive(QueryableByName)]
struct EpochRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    epoch_seconds: Option<i64>,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

/// Minimal isolated Postgres instance for updater process integration tests.
struct TempPostgres {
    data_dir: PathBuf,
    port: u16,
    db_name: String,
}

impl TempPostgres {
    fn start() -> Self {
        assert_binary_exists("initdb");
        assert_binary_exists("pg_ctl");
        assert_binary_exists("createdb");
        assert_binary_exists("dropdb");

        let unique = unique_suffix();
        // Keep the unix socket directory path short enough for Postgres' `sun_path` limits.
        let data_dir = std::env::temp_dir().join(format!("shn_pg_{unique}"));
        fs::create_dir_all(&data_dir).expect("failed to create temporary postgres data dir");

        run_checked(
            Command::new("initdb")
                .arg("-D")
                .arg(&data_dir)
                .arg("-A")
                .arg("trust")
                .arg("-U")
                .arg("postgres")
                .arg("--encoding=UTF8")
                .arg("--no-instructions"),
            "initdb",
        );

        let port = free_tcp_port();
        run_checked_status(
            Command::new("pg_ctl")
                .arg("-D")
                .arg(&data_dir)
                .arg("-o")
                .arg(format!(
                    "-F -p {port} -h 127.0.0.1 -k {}",
                    data_dir.display()
                ))
                .arg("-w")
                .arg("start"),
            "pg_ctl start",
        );

        let db_name = format!("updater_e2e_{unique}");
        run_checked(
            Command::new("createdb")
                .arg("-h")
                .arg("127.0.0.1")
                .arg("-p")
                .arg(port.to_string())
                .arg("-U")
                .arg("postgres")
                .arg(&db_name),
            "createdb",
        );

        Self {
            data_dir,
            port,
            db_name,
        }
    }

    fn database_url(&self) -> String {
        format!(
            "postgresql://postgres@127.0.0.1:{}/{}",
            self.port, self.db_name
        )
    }
}

impl Drop for TempPostgres {
    fn drop(&mut self) {
        let _ = Command::new("dropdb")
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(self.port.to_string())
            .arg("-U")
            .arg("postgres")
            .arg(&self.db_name)
            .status();

        let _ = Command::new("pg_ctl")
            .arg("-D")
            .arg(&self.data_dir)
            .arg("-m")
            .arg("immediate")
            .arg("-w")
            .arg("stop")
            .status();

        let _ = fs::remove_dir_all(&self.data_dir);
    }
}

/// Shared state for a local Firebase-like API and SSE updates endpoint.
struct MockFirebaseState {
    maxitem: i64,
    items: HashMap<i64, Value>,
    updates_payload: String,
    failing_item_id: Option<i64>,
    failing_status: Option<u16>,
}

/// In-process mock upstream service for updater process tests.
struct MockFirebaseServer {
    base_url: String,
    task: tokio::task::JoinHandle<()>,
}

impl MockFirebaseServer {
    async fn start() -> Self {
        Self::start_with_config(default_mock_items(), vec![1, 2, 3], None).await
    }

    async fn start_with_realtime_failure(item_id: i64, status: u16) -> Self {
        let mut items = default_mock_items();
        items.insert(
            item_id,
            serde_json::json!({
                "id": item_id,
                "type": "story",
                "title": "failing item",
                "time": current_unix_epoch_seconds() - 5
            }),
        );
        Self::start_with_config(items, vec![item_id], Some((item_id, status))).await
    }

    async fn start_with_config(
        items: HashMap<i64, Value>,
        update_ids: Vec<i64>,
        failing_item: Option<(i64, u16)>,
    ) -> Self {
        let maxitem = items.keys().copied().max().unwrap_or(1);
        let (failing_item_id, failing_status) = failing_item.unzip();
        let state = Arc::new(MockFirebaseState {
            maxitem,
            items,
            updates_payload: serde_json::json!({
                "path": "/",
                "data": { "items": update_ids }
            })
            .to_string(),
            failing_item_id,
            failing_status,
        });

        let app = Router::new()
            .route("/v0/maxitem.json", get(maxitem_handler))
            .route("/v0/item/{item_path}", get(item_handler))
            .route("/v0/updates.json", get(updates_handler))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind mock firebase listener");
        let addr = listener
            .local_addr()
            .expect("mock firebase listener should have a local address");

        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock firebase axum server failed");
        });

        Self {
            base_url: format!("http://{addr}/v0"),
            task,
        }
    }
}

impl Drop for MockFirebaseServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn maxitem_handler(State(state): State<Arc<MockFirebaseState>>) -> String {
    state.maxitem.to_string()
}

async fn item_handler(
    AxumPath(item_path): AxumPath<String>,
    State(state): State<Arc<MockFirebaseState>>,
) -> (StatusCode, Json<Value>) {
    let item_id = item_path
        .trim_end_matches(".json")
        .parse::<i64>()
        .expect("mock item path should contain numeric id");

    if state.failing_item_id == Some(item_id) {
        let status = state.failing_status.unwrap_or(503);
        let code = StatusCode::from_u16(status).expect("configured status should be valid");
        return (
            code,
            Json(serde_json::json!({
                "error": format!("scripted status {status} for item {item_id}")
            })),
        );
    }

    let payload = state.items.get(&item_id).cloned().unwrap_or(Value::Null);
    (StatusCode::OK, Json(payload))
}

async fn updates_handler(
    State(state): State<Arc<MockFirebaseState>>,
) -> Sse<BoxStream<'static, Result<Event, Infallible>>> {
    let event = Event::default()
        .event("put")
        .data(state.updates_payload.clone());
    let stream = stream::iter(vec![Ok(event)])
        .chain(stream::pending())
        .boxed();
    Sse::new(stream)
}

#[tokio::test]
async fn updater_restart_reloads_persisted_replay_anchor() {
    let pg = TempPostgres::start();
    run_pg_migrations(&pg.database_url());
    let mock = MockFirebaseServer::start().await;

    let mut first = spawn_updater(&pg.database_url(), &mock.base_url, 1, false);
    let first_persisted_epoch =
        wait_for_updater_state_epoch(&pg.database_url(), Duration::from_secs(20)).await;
    assert!(
        first_persisted_epoch > 0,
        "expected first updater run to persist last_sse_event_at"
    );

    terminate_with_sigterm(&mut first);
    let first_status = first
        .wait()
        .expect("failed to wait for first updater process");
    assert!(
        first_status.success(),
        "first updater process should exit cleanly on SIGTERM, got {first_status}"
    );

    let persisted_after_first = load_updater_state_epoch(&pg.database_url())
        .expect("expected updater_state row after first run");

    let mut second = spawn_updater(&pg.database_url(), &mock.base_url, 60, true);
    tokio::time::sleep(Duration::from_secs(3)).await;
    terminate_with_sigterm(&mut second);
    let output = second
        .wait_with_output()
        .expect("failed to collect second updater process output");

    assert!(
        output.status.success(),
        "second updater process should exit cleanly on SIGTERM; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let replay_anchor_epoch = extract_startup_replay_anchor_epoch(&output)
        .expect("expected startup_rescan_started log with replay_anchor_epoch");
    assert_eq!(
        replay_anchor_epoch, persisted_after_first,
        "second run should start rescan from persisted updater_state anchor"
    );
}

#[tokio::test]
async fn updater_realtime_failure_persists_shared_dlq_record() {
    let pg = TempPostgres::start();
    run_pg_migrations(&pg.database_url());
    let mock = MockFirebaseServer::start_with_realtime_failure(99, 503).await;

    let mut updater = spawn_updater(&pg.database_url(), &mock.base_url, 60, false);
    wait_for_realtime_dlq_record(&pg.database_url(), 99, Duration::from_secs(20)).await;

    terminate_with_sigterm(&mut updater);
    let status = updater
        .wait()
        .expect("failed waiting for updater process shutdown");
    assert!(
        status.success(),
        "updater should exit cleanly after SIGTERM, got {status}"
    );
}

/// Starts the real updater binary with a compact config suitable for e2e tests.
fn spawn_updater(
    database_url: &str,
    hn_api_url: &str,
    persist_interval_seconds: u64,
    capture_output: bool,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_catchup_worker"));
    cmd.env("DATABASE_URL", database_url);
    cmd.env("HN_API_URL", hn_api_url);
    cmd.env("LOG_FORMAT", "json");
    cmd.env("RUST_LOG", "info");
    cmd.args([
        "updater",
        "--metrics-bind",
        "127.0.0.1:0",
        "--realtime-workers",
        "2",
        "--channel-capacity",
        "64",
        "--startup-rescan-days",
        "1",
        "--persist-interval-seconds",
        &persist_interval_seconds.to_string(),
        "--catchup-workers",
        "2",
        "--catchup-segment-width",
        "100",
        "--catchup-global-rps",
        "250",
        "--catchup-batch-size",
        "50",
        "--retry-attempts",
        "1",
        "--retry-initial-ms",
        "0",
        "--retry-max-ms",
        "0",
        "--retry-jitter-ms",
        "0",
    ]);

    if capture_output {
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
    } else {
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());
    }

    cmd.spawn().expect("failed to spawn updater process")
}

fn run_pg_migrations(database_url: &str) {
    let mut conn =
        PgConnection::establish(database_url).expect("failed to connect to postgres for e2e setup");
    run_postgres_migrations(&mut conn);
}

fn default_mock_items() -> HashMap<i64, Value> {
    let mut items = HashMap::new();
    items.insert(
        1,
        serde_json::json!({
            "id": 1,
            "type": "story",
            "title": "one",
            "time": current_unix_epoch_seconds() - 60
        }),
    );
    items.insert(
        2,
        serde_json::json!({
            "id": 2,
            "type": "story",
            "title": "two",
            "time": current_unix_epoch_seconds() - 30
        }),
    );
    items.insert(
        3,
        serde_json::json!({
            "id": 3,
            "type": "comment",
            "text": "three",
            "parent": 2,
            "time": current_unix_epoch_seconds() - 15
        }),
    );
    items
}

async fn wait_for_updater_state_epoch(database_url: &str, timeout: Duration) -> i64 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(epoch) = load_updater_state_epoch(database_url) {
            return epoch;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for updater_state persistence"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_realtime_dlq_record(database_url: &str, item_id: i64, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut conn = PgConnection::establish(database_url)
            .expect("failed to connect to postgres for realtime DLQ assertion");
        let count: CountRow = sql_query(
            "SELECT COUNT(*) AS count \
             FROM ingest_dlq_items \
             WHERE source = 'realtime' AND state = 'retry_wait' AND item_id = $1",
        )
        .bind::<BigInt, _>(item_id)
        .get_result(&mut conn)
        .expect("failed to query realtime DLQ rows");
        if count.count > 0 {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for realtime DLQ record for item {item_id}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn load_updater_state_epoch(database_url: &str) -> Option<i64> {
    let mut conn = PgConnection::establish(database_url)
        .expect("failed to connect to postgres for assertions");
    let mut rows: Vec<EpochRow> = sql_query(
        "SELECT EXTRACT(EPOCH FROM last_sse_event_at)::BIGINT AS epoch_seconds \
         FROM updater_state WHERE id = 1",
    )
    .load(&mut conn)
    .expect("failed to read updater_state");
    rows.pop().and_then(|row| row.epoch_seconds)
}

fn extract_startup_replay_anchor_epoch(output: &Output) -> Option<i64> {
    for bytes in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(bytes);
        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let Some(event) = value.get("event").and_then(|v| v.as_str()) else {
                continue;
            };
            if event == "startup_rescan_started" {
                return value.get("replay_anchor_epoch").and_then(|v| v.as_i64());
            }
        }
    }
    None
}

fn terminate_with_sigterm(child: &mut Child) {
    let pid = child.id().to_string();
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid)
        .status()
        .expect("failed to send SIGTERM to updater process");
    assert!(status.success(), "kill -TERM should succeed");
}

fn assert_binary_exists(name: &str) {
    let status = Command::new("which")
        .arg(name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to execute `which`");
    assert!(
        status.success(),
        "required binary `{name}` is missing; install PostgreSQL CLI tools"
    );
}

fn run_checked(cmd: &mut Command, description: &str) {
    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn subprocess");
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "{description} failed (status {}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status
        );
    }
}

fn run_checked_status(cmd: &mut Command, description: &str) {
    let status = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("failed to spawn subprocess");
    assert!(status.success(), "{description} failed (status {status})");
}

fn free_tcp_port() -> u16 {
    let listener =
        StdTcpListener::bind("127.0.0.1:0").expect("failed to bind temporary local port");
    listener
        .local_addr()
        .expect("failed to read local bind address")
        .port()
}

fn current_unix_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time went backwards")
        .as_secs() as i64
}

fn unique_suffix() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time went backwards")
        .as_nanos();
    format!("{}_{}", std::process::id(), now)
}
