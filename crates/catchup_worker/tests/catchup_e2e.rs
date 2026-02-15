use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Text};
use diesel_migrations::{FileBasedMigrations, MigrationHarness};
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct StatusRow {
    #[diesel(sql_type = Text)]
    status: String,
}

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
        let data_dir = std::env::temp_dir().join(format!("search_hn_pg_{unique}"));
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

        let db_name = format!("catchup_e2e_{unique}");
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

struct MockFirebaseProcess {
    child: Child,
    base_url: String,
}

impl MockFirebaseProcess {
    async fn start(fixture_path: &Path) -> Self {
        assert!(
            binary_exists("uv"),
            "required binary `uv` is missing for catchup e2e tests"
        );
        let port = free_tcp_port();
        let workspace_root = workspace_root();
        let project = workspace_root.join("packages/mock_firebase");
        let script = project.join("main.py");

        let child = Command::new("uv")
            .arg("run")
            .arg("--project")
            .arg(&project)
            .arg("python")
            .arg(&script)
            .arg("--fixture")
            .arg(fixture_path)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn mock firebase process");

        let mut process = Self {
            child,
            base_url: format!("http://127.0.0.1:{port}/v0"),
        };

        process.wait_until_ready().await;
        process
    }

    async fn wait_until_ready(&mut self) {
        let client = reqwest::Client::new();
        let health_url = self.base_url.replace("/v0", "/healthz");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .expect("failed to poll mock firebase process")
            {
                panic!("mock firebase exited before ready: {status}");
            }

            if let Ok(response) = client.get(&health_url).send().await {
                if response.status().is_success() {
                    return;
                }
            }

            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for mock firebase readiness at {health_url}");
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for MockFirebaseProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn catchup_only_happy_path_with_transient_errors() {
    let pg = TempPostgres::start();
    run_pg_migrations(&pg.database_url());
    let fixture = write_fixture(
        r#"
{
  "maxitem": 5,
  "items": {
    "1": { "id": 1, "type": "story", "title": "one" },
    "2": { "id": 2, "type": "story", "title": "two", "kids": [4] },
    "3": null,
    "4": { "id": 4, "type": "comment", "text": "child comment" },
    "5": { "id": 5, "type": "story", "deleted": true, "title": "deleted story" }
  },
  "scripts": {
    "2": [429, 429, 200],
    "4": [503, 200]
  }
}
"#,
    );
    let mock = MockFirebaseProcess::start(&fixture).await;

    let status = run_catchup_only(
        &pg.database_url(),
        &mock.base_url,
        &["--catchup-start", "1"],
    );
    assert!(
        status.success(),
        "catchup_only should succeed, got: {status}"
    );

    let mut conn = PgConnection::establish(&pg.database_url())
        .expect("failed to connect to temporary postgres for assertions");

    let item_count: CountRow = sql_query("SELECT COUNT(*) AS count FROM items")
        .get_result(&mut conn)
        .expect("failed to count items");
    assert_eq!(
        item_count.count, 4,
        "expected one missing item to stay absent"
    );

    let segment_statuses: Vec<StatusRow> =
        sql_query("SELECT status FROM ingest_segments ORDER BY segment_id")
            .load(&mut conn)
            .expect("failed to query segment statuses");
    assert_eq!(segment_statuses.len(), 1);
    assert_eq!(segment_statuses[0].status, "done");

    let terminal_missing: CountRow = sql_query(
        "SELECT COUNT(*) AS count FROM ingest_exceptions WHERE state = 'terminal_missing'",
    )
    .get_result(&mut conn)
    .expect("failed to count terminal missing exceptions");
    assert_eq!(terminal_missing.count, 1);
}

#[tokio::test]
async fn catchup_only_fatal_failure_marks_dead_letter_and_exits_nonzero() {
    let pg = TempPostgres::start();
    run_pg_migrations(&pg.database_url());
    let fixture = write_fixture(
        r#"
{
  "maxitem": 3,
  "items": {
    "1": { "id": 1, "type": "story", "title": "one" },
    "2": { "id": 2, "type": "story", "title": "two" },
    "3": { "id": 3, "type": "story", "title": "three" }
  },
  "scripts": {
    "2": [401]
  }
}
"#,
    );
    let mock = MockFirebaseProcess::start(&fixture).await;

    let status = run_catchup_only(
        &pg.database_url(),
        &mock.base_url,
        &["--catchup-start", "1"],
    );
    assert!(
        !status.success(),
        "catchup_only should fail on fatal upstream auth error"
    );

    let mut conn = PgConnection::establish(&pg.database_url())
        .expect("failed to connect to temporary postgres for assertions");

    let dead_letter_segments: CountRow =
        sql_query("SELECT COUNT(*) AS count FROM ingest_segments WHERE status = 'dead_letter'")
            .get_result(&mut conn)
            .expect("failed to count dead-letter segments");
    assert_eq!(dead_letter_segments.count, 1);

    let dead_letter_exceptions: CountRow =
        sql_query("SELECT COUNT(*) AS count FROM ingest_exceptions WHERE state = 'dead_letter'")
            .get_result(&mut conn)
            .expect("failed to count dead-letter exceptions");
    assert_eq!(dead_letter_exceptions.count, 1);
}

fn run_catchup_only(
    database_url: &str,
    hn_api_base: &str,
    extra_args: &[&str],
) -> std::process::ExitStatus {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_catchup_only"));
    cmd.env("DATABASE_URL", database_url);
    cmd.env("HN_API_URL", hn_api_base);
    cmd.args(["--metrics-bind", "127.0.0.1:0"]);
    cmd.args(extra_args);
    cmd.status().expect("failed to run catchup_only binary")
}

fn run_pg_migrations(database_url: &str) {
    let migrations_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let migrations = FileBasedMigrations::from_path(&migrations_dir)
        .expect("failed to load postgres migrations for e2e");

    let mut conn =
        PgConnection::establish(database_url).expect("failed to connect to postgres for e2e setup");
    conn.run_pending_migrations(migrations)
        .expect("failed to run postgres migrations for e2e");
}

fn write_fixture(contents: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("search_hn_fixture_{}", unique_suffix()));
    fs::create_dir_all(&dir).expect("failed to create temporary fixture directory");
    let path = dir.join("fixture.json");
    fs::write(&path, contents).expect("failed to write fixture file");
    path
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate has no parent")
        .parent()
        .expect("workspace root not found")
        .to_path_buf()
}

fn binary_exists(name: &str) -> bool {
    let status = Command::new("which")
        .arg(name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to execute `which`");
    status.success()
}

fn assert_binary_exists(name: &str) {
    assert!(
        binary_exists(name),
        "required binary `{name}` is missing; install PostgreSQL CLI tools and uv"
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
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind temporary local port");
    listener
        .local_addr()
        .expect("failed to read local bind address")
        .port()
}

fn unique_suffix() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time went backwards")
        .as_nanos();
    format!("{}_{}", std::process::id(), now)
}
