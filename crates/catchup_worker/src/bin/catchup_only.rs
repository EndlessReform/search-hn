use catchup_worker_lib::{
    db::build_db_pool,
    sync_service::{types::BatchPolicy, types::IngestWorkerConfig, types::RetryPolicy},
    sync_service::{CatchupOrchestratorConfig, SyncService},
};
use clap::Parser;
use diesel::{pg::PgConnection, Connection};
use diesel_migrations::{FileBasedMigrations, MigrationHarness};
use dotenv::dotenv;
use std::env;
use std::time::Duration;

const DEFAULT_HN_API_URL: &str = "https://hacker-news.firebaseio.com/v0";

#[derive(Debug, Parser)]
#[command(about = "Run catchup once and exit (no realtime listener)")]
struct Args {
    #[arg(long = "database-url")]
    database_url: Option<String>,
    #[arg(long = "hn-api-url")]
    hn_api_url: Option<String>,

    #[arg(long = "start-id", alias = "catchup-start")]
    start_id: Option<i64>,
    #[arg(long = "limit", alias = "catchup-amt")]
    limit: Option<i64>,

    #[arg(long, default_value_t = 16)]
    workers: usize,
    #[arg(long = "segment-width", default_value_t = 1000)]
    segment_width: i64,
    #[arg(long = "queue-capacity", default_value_t = 1024)]
    queue_capacity: usize,
    #[arg(long = "batch-size", default_value_t = 500)]
    batch_size: usize,

    #[arg(long = "retry-attempts", default_value_t = 5)]
    retry_attempts: u32,
    #[arg(long = "retry-initial-ms", default_value_t = 100)]
    retry_initial_ms: u64,
    #[arg(long = "retry-max-ms", default_value_t = 5000)]
    retry_max_ms: u64,
    #[arg(long = "retry-jitter-ms", default_value_t = 25)]
    retry_jitter_ms: u64,

    #[arg(long = "log-level", default_value = "info")]
    log_level: String,
}

fn run_migrations(database_url: &str) -> Result<(), String> {
    let migrations_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
    let migrations = FileBasedMigrations::from_path(migrations_dir)
        .map_err(|err| format!("failed to load migrations: {err}"))?;

    let mut conn = PgConnection::establish(database_url)
        .map_err(|err| format!("failed to connect for migrations: {err}"))?;
    conn.run_pending_migrations(migrations)
        .map_err(|err| format!("failed to run migrations: {err}"))?;
    Ok(())
}

fn resolve_database_url(args: &Args) -> Result<String, String> {
    if let Some(value) = &args.database_url {
        return Ok(value.clone());
    }

    env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is required (env var or --database-url)".to_string())
}

fn resolve_hn_api_url(args: &Args) -> String {
    if let Some(value) = &args.hn_api_url {
        return value.clone();
    }
    env::var("HN_API_URL").unwrap_or_else(|_| DEFAULT_HN_API_URL.to_string())
}

fn validate_args(args: &Args) -> Result<(), String> {
    if let Some(limit) = args.limit {
        if limit <= 0 {
            return Err(format!("--limit must be > 0, got {limit}"));
        }
    }
    if let Some(start_id) = args.start_id {
        if start_id <= 0 {
            return Err(format!("--start-id must be > 0, got {start_id}"));
        }
    }
    if args.workers == 0 {
        return Err("--workers must be > 0".to_string());
    }
    if args.segment_width <= 0 {
        return Err(format!(
            "--segment-width must be > 0, got {}",
            args.segment_width
        ));
    }
    if args.queue_capacity == 0 {
        return Err("--queue-capacity must be > 0".to_string());
    }
    if args.batch_size == 0 {
        return Err("--batch-size must be > 0".to_string());
    }
    if args.retry_attempts == 0 {
        return Err("--retry-attempts must be > 0".to_string());
    }
    if args.retry_max_ms < args.retry_initial_ms {
        return Err(format!(
            "--retry-max-ms ({}) must be >= --retry-initial-ms ({})",
            args.retry_max_ms, args.retry_initial_ms
        ));
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let args = Args::parse();
    if env::var("RUST_LOG").is_err() {
        // Keep the dev loop quick: INFO by default, override with RUST_LOG when needed.
        env_logger::Builder::from_env(
            env_logger::Env::default().default_filter_or(args.log_level.clone()),
        )
        .init();
    } else {
        env_logger::init();
    }

    if let Err(err) = validate_args(&args) {
        eprintln!("{err}");
        std::process::exit(2);
    }

    let db_url = match resolve_database_url(&args) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };
    let hn_api_url = resolve_hn_api_url(&args);

    if let Err(err) = run_migrations(&db_url) {
        eprintln!("{err}");
        std::process::exit(1);
    }

    let pool = match build_db_pool(&db_url).await {
        Ok(value) => value,
        Err(err) => {
            eprintln!("failed to build db pool: {err}");
            std::process::exit(1);
        }
    };

    let service = SyncService::new(db_url.clone(), hn_api_url, pool, args.workers);

    let orchestrator_config = CatchupOrchestratorConfig {
        worker_count: args.workers,
        segment_width: args.segment_width,
        queue_capacity: args.queue_capacity,
        ingest_worker: IngestWorkerConfig {
            retry_policy: RetryPolicy {
                max_attempts: args.retry_attempts,
                initial_backoff: Duration::from_millis(args.retry_initial_ms),
                max_backoff: Duration::from_millis(args.retry_max_ms),
                jitter: Duration::from_millis(args.retry_jitter_ms),
            },
            batch_policy: BatchPolicy {
                max_items: args.batch_size,
            },
        },
    };

    if let Err(err) = service
        .catchup_with_orchestrator_config(args.limit, args.start_id, orchestrator_config)
        .await
    {
        eprintln!("catchup failed: {err}");
        std::process::exit(1);
    }
}
