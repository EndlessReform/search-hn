use catchup_worker_lib::{
    db::build_db_pool,
    logging::init_logging,
    sync_service::{types::BatchPolicy, types::IngestWorkerConfig, types::RetryPolicy},
    sync_service::{CatchupOrchestratorConfig, SyncService},
};
use clap::Parser;
use dotenv::dotenv;
use std::env;
use std::time::Duration;
use tracing::{error, info};

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
    #[arg(long = "end-id", alias = "catchup-end")]
    end_id: Option<i64>,
    #[arg(long = "limit", alias = "catchup-amt")]
    limit: Option<i64>,
    #[arg(long, default_value_t = false)]
    /// Debug only: do not clamp target max to upstream maxitem.
    ///
    /// Requires `--end-id` or `--limit` so the run stays bounded.
    ignore_highest: bool,

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
    if let Some(end_id) = args.end_id {
        if end_id <= 0 {
            return Err(format!("--end-id must be > 0, got {end_id}"));
        }
        if let Some(start_id) = args.start_id {
            if end_id < start_id {
                return Err(format!(
                    "--end-id ({end_id}) must be >= --start-id ({start_id})"
                ));
            }
        }
    }
    if args.limit.is_some() && args.end_id.is_some() {
        return Err("--limit and --end-id are mutually exclusive".to_string());
    }
    if args.ignore_highest && args.limit.is_none() && args.end_id.is_none() {
        return Err("--ignore-highest requires --limit or --end-id".to_string());
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
    let logging_context = init_logging("catchup_worker", "catchup_only", &args.log_level);
    let run_span = tracing::info_span!(
        "worker_run",
        service = %logging_context.service,
        environment = %logging_context.environment,
        mode = %logging_context.mode,
        run_id = %logging_context.run_id
    );
    let _run_guard = run_span.enter();
    info!(event = "catchup_only_starting", "starting catchup-only run");

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
        force_replay_window: args.ignore_highest,
    };

    if let Err(err) = service
        .catchup_with_orchestrator_config(
            args.limit,
            args.start_id,
            args.end_id,
            args.ignore_highest,
            orchestrator_config,
        )
        .await
    {
        error!(
            event = "catchup_only_failed",
            error = %err,
            "catchup-only run failed"
        );
        eprintln!("catchup failed: {err}");
        std::process::exit(1);
    }

    info!(
        event = "catchup_only_complete",
        "catchup-only run completed"
    );
}
