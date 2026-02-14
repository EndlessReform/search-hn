use catchup_worker_lib::{
    build_info,
    db::build_db_pool,
    logging::init_logging,
    server::setup_server_with_addr,
    state::AppState,
    sync_service::{types::BatchPolicy, types::IngestWorkerConfig, types::RetryPolicy},
    sync_service::{CatchupOrchestratorConfig, SyncService},
};
use clap::Parser;
use dotenv::dotenv;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

const DEFAULT_HN_API_URL: &str = "https://hacker-news.firebaseio.com/v0";
const ASSUMED_FETCH_LATENCY_MS: u32 = 50;
const WORKER_OVERPROVISION_NUMERATOR: u32 = 9;
const WORKER_OVERPROVISION_DENOMINATOR: u32 = 5;

#[derive(Debug, Parser)]
#[command(
    about = "Run catchup once and exit (no realtime listener)",
    version = build_info::VERSION_WITH_COMMIT,
    long_version = build_info::VERSION_WITH_COMMIT
)]
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

    #[arg(long = "workers", alias = "num-workers")]
    workers: Option<usize>,
    #[arg(long = "segment-width", default_value_t = 1000)]
    segment_width: i64,
    #[arg(long = "queue-capacity", default_value_t = 1024)]
    queue_capacity: usize,
    #[arg(long = "global-rps", default_value_t = 250)]
    global_rps: u32,
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
    #[arg(long = "metrics-bind", default_value = "0.0.0.0:3000")]
    metrics_bind: String,
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

/// Derives a practical worker count from a global RPS budget.
///
/// We model required in-flight fetches as `rps * latency` and add a small overprovision factor
/// to absorb jitter and DB flush pauses:
/// `workers = ceil(ceil(global_rps * 50ms / 1000ms) * 1.8)`.
fn derive_worker_count_from_rps(global_rps: u32) -> usize {
    fn ceil_div_u64(numerator: u64, denominator: u64) -> u64 {
        numerator
            .saturating_add(denominator.saturating_sub(1))
            .saturating_div(denominator)
    }

    let in_flight = ceil_div_u64(
        (global_rps as u64).saturating_mul(ASSUMED_FETCH_LATENCY_MS as u64),
        1000,
    )
    .max(1);
    let overprovisioned = ceil_div_u64(
        in_flight.saturating_mul(WORKER_OVERPROVISION_NUMERATOR as u64),
        WORKER_OVERPROVISION_DENOMINATOR as u64,
    )
    .max(1);

    usize::try_from(overprovisioned).unwrap_or(usize::MAX)
}

fn resolve_worker_count(args: &Args) -> usize {
    args.workers
        .unwrap_or_else(|| derive_worker_count_from_rps(args.global_rps))
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
    if let Some(workers) = args.workers {
        if workers == 0 {
            return Err("--workers/--num-workers must be > 0".to_string());
        }
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
    if args.global_rps == 0 {
        return Err("--global-rps must be > 0".to_string());
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
    args.metrics_bind.parse::<SocketAddr>().map_err(|err| {
        format!(
            "invalid --metrics-bind address `{}`: {err}",
            args.metrics_bind
        )
    })?;

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
        run_id = %logging_context.run_id,
        build_version = %logging_context.build_version,
        build_commit = %logging_context.build_commit
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
    let resolved_workers = resolve_worker_count(&args);
    let worker_source = if args.workers.is_some() {
        "explicit"
    } else {
        "derived_from_global_rps"
    };
    info!(
        event = "catchup_worker_count_resolved",
        worker_count = resolved_workers,
        worker_count_source = worker_source,
        global_rps = args.global_rps,
        assumed_fetch_latency_ms = ASSUMED_FETCH_LATENCY_MS,
        overprovision_ratio_numerator = WORKER_OVERPROVISION_NUMERATOR,
        overprovision_ratio_denominator = WORKER_OVERPROVISION_DENOMINATOR,
        "resolved catchup worker count"
    );

    let pool = match build_db_pool(&db_url).await {
        Ok(value) => value,
        Err(err) => {
            eprintln!("failed to build db pool: {err}");
            std::process::exit(1);
        }
    };
    let metrics_addr = args
        .metrics_bind
        .parse::<SocketAddr>()
        .expect("metrics bind address validated earlier");
    let app_state = Arc::new(AppState::new(pool.clone(), CancellationToken::new()));
    let metrics_server_handle = match setup_server_with_addr(app_state, metrics_addr).await {
        Ok(handle) => handle,
        Err(err) => {
            eprintln!("failed to start metrics endpoint on {metrics_addr}: {err}");
            std::process::exit(1);
        }
    };
    info!(
        event = "metrics_server_started",
        bind = %metrics_addr,
        "started metrics endpoint"
    );

    let service = SyncService::new(hn_api_url, pool, resolved_workers);

    let orchestrator_config = CatchupOrchestratorConfig {
        worker_count: resolved_workers,
        segment_width: args.segment_width,
        queue_capacity: args.queue_capacity,
        global_rps_limit: args.global_rps,
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
        metrics_server_handle.abort();
        std::process::exit(1);
    }

    info!(
        event = "catchup_only_complete",
        "catchup-only run completed"
    );
    metrics_server_handle.abort();
}

#[cfg(test)]
mod tests {
    use super::derive_worker_count_from_rps;

    #[test]
    fn derived_workers_match_target_for_250_rps() {
        assert_eq!(derive_worker_count_from_rps(250), 24);
    }

    #[test]
    fn derived_workers_scale_monotonically() {
        assert!(derive_worker_count_from_rps(100) < derive_worker_count_from_rps(250));
        assert!(derive_worker_count_from_rps(250) < derive_worker_count_from_rps(500));
    }
}
