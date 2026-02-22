use catchup_worker_lib::{
    build_info,
    commands::{run_catchup_once, CatchupArgs},
    db::build_db_pool,
    firebase_listener::FirebaseListener,
    logging::{format_error_report, init_logging},
    server::{monitoring::REALTIME_METRICS, setup_server_with_addr},
    state::AppState,
    sync_service::{
        types::{BatchPolicy, IngestWorkerConfig, RetryPolicy},
        updater_state::{
            find_rescan_start_id_from_time, load_last_sse_event_epoch, save_last_sse_event_epoch,
        },
        CatchupOrchestratorConfig, SyncService,
    },
};
use clap::{Parser, Subcommand};
use dotenv::dotenv;
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const DEFAULT_HN_API_URL: &str = "https://hacker-news.firebaseio.com/v0";
const DEFAULT_DB_POOL_MAX_SIZE: usize = 64;

#[derive(Debug, Parser)]
#[command(
    about = "search-hn ingest runtime",
    version = build_info::VERSION_WITH_COMMIT,
    long_version = build_info::VERSION_WITH_COMMIT
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Long-running updater service (SSE + supervised realtime workers + startup replay).
    Updater(UpdaterArgs),
    /// One-shot catchup run and exit.
    Catchup(CatchupArgs),
}

#[derive(Debug, Parser, Clone)]
struct UpdaterArgs {
    #[arg(long = "database-url")]
    database_url: Option<String>,
    #[arg(long = "hn-api-url")]
    hn_api_url: Option<String>,

    #[arg(long = "log-level", default_value = "info")]
    log_level: String,
    #[arg(long = "metrics-bind", default_value = "0.0.0.0:3000")]
    metrics_bind: String,

    #[arg(long = "db-pool-size", default_value_t = DEFAULT_DB_POOL_MAX_SIZE)]
    db_pool_size: usize,
    #[arg(long = "realtime-workers", default_value_t = 8)]
    realtime_workers: usize,
    #[arg(long = "channel-capacity", default_value_t = 4096)]
    channel_capacity: usize,

    #[arg(long = "startup-rescan-days", default_value_t = 3)]
    startup_rescan_days: i64,
    #[arg(long = "persist-interval-seconds", default_value_t = 60)]
    persist_interval_seconds: u64,

    #[arg(long = "catchup-workers", default_value_t = 24)]
    catchup_workers: usize,
    #[arg(long = "catchup-segment-width", default_value_t = 1000)]
    catchup_segment_width: i64,
    #[arg(long = "catchup-queue-capacity")]
    catchup_queue_capacity: Option<usize>,
    #[arg(long = "catchup-global-rps", default_value_t = 250)]
    catchup_global_rps: u32,
    #[arg(long = "catchup-batch-size", default_value_t = 500)]
    catchup_batch_size: usize,

    #[arg(long = "retry-attempts", default_value_t = 5)]
    retry_attempts: u32,
    #[arg(long = "retry-initial-ms", default_value_t = 100)]
    retry_initial_ms: u64,
    #[arg(long = "retry-max-ms", default_value_t = 5000)]
    retry_max_ms: u64,
    #[arg(long = "retry-jitter-ms", default_value_t = 25)]
    retry_jitter_ms: u64,
}

/// Gracefully shuts down the application when a SIGTERM or SIGINT signal is received.
async fn handle_shutdown_signals(state: Arc<AppState>) {
    let mut sigterm =
        signal(SignalKind::terminate()).expect("Failed to register SIGTERM signal handler");
    let mut sigint =
        signal(SignalKind::interrupt()).expect("Failed to register SIGINT signal handler");

    tokio::select! {
        _ = sigterm.recv() => {
            info!(event = "shutdown_signal_received", signal = "SIGTERM", "shutdown signal received");
        }
        _ = sigint.recv() => {
            info!(event = "shutdown_signal_received", signal = "SIGINT", "shutdown signal received");
        }
    }

    state.shutdown_token.cancel();
}

fn resolve_database_url(database_url: &Option<String>) -> Result<String, String> {
    if let Some(value) = database_url {
        return Ok(value.clone());
    }

    env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is required (env var or --database-url)".to_string())
}

fn resolve_hn_api_url(hn_api_url: &Option<String>) -> String {
    if let Some(value) = hn_api_url {
        return value.clone();
    }
    env::var("HN_API_URL").unwrap_or_else(|_| DEFAULT_HN_API_URL.to_string())
}

fn current_unix_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_secs() as i64
}

fn validate_updater_args(args: &UpdaterArgs) -> Result<(), String> {
    if args.db_pool_size == 0 {
        return Err("--db-pool-size must be > 0".to_string());
    }
    if args.realtime_workers == 0 {
        return Err("--realtime-workers must be > 0".to_string());
    }
    if args.channel_capacity == 0 {
        return Err("--channel-capacity must be > 0".to_string());
    }
    if args.startup_rescan_days <= 0 {
        return Err("--startup-rescan-days must be > 0".to_string());
    }
    if args.persist_interval_seconds == 0 {
        return Err("--persist-interval-seconds must be > 0".to_string());
    }
    if args.catchup_workers == 0 {
        return Err("--catchup-workers must be > 0".to_string());
    }
    if args.catchup_segment_width <= 0 {
        return Err("--catchup-segment-width must be > 0".to_string());
    }
    if let Some(queue_capacity) = args.catchup_queue_capacity {
        if queue_capacity == 0 {
            return Err("--catchup-queue-capacity must be > 0".to_string());
        }
    }
    if args.catchup_global_rps == 0 {
        return Err("--catchup-global-rps must be > 0".to_string());
    }
    if args.catchup_batch_size == 0 {
        return Err("--catchup-batch-size must be > 0".to_string());
    }
    if args.retry_attempts == 0 {
        return Err("--retry-attempts must be > 0".to_string());
    }
    if args.retry_max_ms < args.retry_initial_ms {
        return Err("--retry-max-ms must be >= --retry-initial-ms".to_string());
    }
    args.metrics_bind.parse::<SocketAddr>().map_err(|err| {
        format!(
            "invalid --metrics-bind address `{}`: {err}",
            args.metrics_bind
        )
    })?;

    Ok(())
}

async fn run_updater(args: UpdaterArgs) -> i32 {
    if let Err(err) = validate_updater_args(&args) {
        eprintln!("{err}");
        return 2;
    }

    let db_url = match resolve_database_url(&args.database_url) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let hn_api_url = resolve_hn_api_url(&args.hn_api_url);

    let logging_context = init_logging("catchup_worker", "updater", &args.log_level);
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

    let pool = match build_db_pool(&db_url, args.db_pool_size).await {
        Ok(value) => value,
        Err(err) => {
            let error_report = format_error_report(&err);
            error!(
                event = "updater_db_pool_build_failed",
                error = %err,
                error_debug = ?err,
                error_report = %error_report,
                "failed to build db pool"
            );
            eprintln!("failed to build db pool: {err}");
            eprintln!("{error_report}");
            return 1;
        }
    };

    let state = Arc::new(AppState::new(pool.clone(), CancellationToken::new()));
    let shutdown_handle = tokio::spawn(handle_shutdown_signals(state.clone()));

    let metrics_addr = args
        .metrics_bind
        .parse::<SocketAddr>()
        .expect("metrics bind address validated earlier");
    let server_handle = match setup_server_with_addr(state.clone(), metrics_addr).await {
        Ok(handle) => handle,
        Err(err) => {
            let error_report = format_error_report(&err);
            error!(
                event = "updater_metrics_server_start_failed",
                bind = %metrics_addr,
                error = %err,
                error_debug = ?err,
                error_report = %error_report,
                "failed to start metrics endpoint"
            );
            eprintln!("failed to start metrics endpoint on {metrics_addr}: {err}");
            eprintln!("{error_report}");
            return 1;
        }
    };

    let sync_service = SyncService::new(
        hn_api_url.clone(),
        pool.clone(),
        logging_context.run_id.clone(),
        args.catchup_workers,
    );

    let persisted_last_event = match load_last_sse_event_epoch(&pool).await {
        Ok(value) => value,
        Err(err) => {
            error!(
                event = "updater_state_load_failed",
                error = %err,
                "failed to load updater_state"
            );
            None
        }
    };

    let now_epoch = current_unix_epoch_seconds();
    let bootstrap_epoch = persisted_last_event.unwrap_or_else(|| {
        now_epoch.saturating_sub(args.startup_rescan_days.saturating_mul(24 * 60 * 60))
    });
    let last_event_epoch = Arc::new(AtomicI64::new(bootstrap_epoch));

    let (sender, receiver) = flume::bounded::<i64>(args.channel_capacity);

    let listener_cancel_token = state.shutdown_token.clone();
    let listener_last_event = Arc::clone(&last_event_epoch);
    let listener_url = hn_api_url.clone();
    let listener_handle = tokio::spawn(async move {
        FirebaseListener::new(listener_url)?
            .listen_to_updates_resilient(sender, listener_cancel_token, listener_last_event)
            .await
    });

    let realtime_worker_config = IngestWorkerConfig {
        retry_policy: RetryPolicy {
            max_attempts: args.retry_attempts,
            initial_backoff: Duration::from_millis(args.retry_initial_ms),
            max_backoff: Duration::from_millis(args.retry_max_ms),
            jitter: Duration::from_millis(args.retry_jitter_ms),
        },
        batch_policy: BatchPolicy {
            max_items: args.catchup_batch_size,
        },
    };

    let workers_cancel_token = state.shutdown_token.clone();
    let workers_handle = tokio::spawn(async move {
        sync_service
            .realtime_update_supervised(
                args.realtime_workers,
                receiver,
                workers_cancel_token,
                realtime_worker_config,
            )
            .await
    });

    let persist_cancel = state.shutdown_token.clone();
    let persist_pool = pool.clone();
    let persist_last_event = Arc::clone(&last_event_epoch);
    let persist_interval = Duration::from_secs(args.persist_interval_seconds);
    let persist_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = persist_cancel.cancelled() => break,
                _ = tokio::time::sleep(persist_interval) => {
                    let epoch = persist_last_event.load(Ordering::Relaxed);
                    if epoch > 0 {
                        if let Err(err) = save_last_sse_event_epoch(&persist_pool, epoch).await {
                            warn!(
                                event = "updater_state_persist_failed",
                                error = %err,
                                "failed to persist last_sse_event_at"
                            );
                        }
                    }
                    if let Some(metrics) = REALTIME_METRICS.get() {
                        let age = current_unix_epoch_seconds().saturating_sub(epoch).max(0);
                        metrics.last_event_age_seconds.set(age);
                    }
                }
            }
        }
    });

    let rescan_pool = pool.clone();
    let rescan_service = SyncService::new(
        hn_api_url.clone(),
        pool,
        logging_context.run_id,
        args.catchup_workers,
    );
    let rescan_cancel = state.shutdown_token.clone();
    let rescan_days = args.startup_rescan_days;
    let rescan_segment_width = args.catchup_segment_width;
    let rescan_queue_capacity = args
        .catchup_queue_capacity
        .unwrap_or_else(|| args.catchup_workers.saturating_mul(2));
    let rescan_global_rps = args.catchup_global_rps;
    let rescan_batch_size = args.catchup_batch_size;
    let rescan_retry_policy = RetryPolicy {
        max_attempts: args.retry_attempts,
        initial_backoff: Duration::from_millis(args.retry_initial_ms),
        max_backoff: Duration::from_millis(args.retry_max_ms),
        jitter: Duration::from_millis(args.retry_jitter_ms),
    };
    tokio::spawn(async move {
        let replay_anchor_epoch = load_last_sse_event_epoch(&rescan_pool)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(current_unix_epoch_seconds);
        let rescan_from_epoch = replay_anchor_epoch.saturating_sub(rescan_days * 24 * 60 * 60);
        let start_id = match find_rescan_start_id_from_time(&rescan_pool, rescan_from_epoch).await {
            Ok(id) => id,
            Err(err) => {
                error!(
                    event = "startup_rescan_window_resolve_failed",
                    error = %err,
                    "failed to resolve startup rescan start_id"
                );
                rescan_cancel.cancel();
                return;
            }
        };

        let started_at = Instant::now();
        info!(
            event = "startup_rescan_started",
            rescan_from_epoch, replay_anchor_epoch, start_id, "starting updater startup rescan"
        );

        let config = CatchupOrchestratorConfig {
            worker_count: args.catchup_workers,
            segment_width: rescan_segment_width,
            queue_capacity: rescan_queue_capacity,
            global_rps_limit: rescan_global_rps,
            ingest_worker: IngestWorkerConfig {
                retry_policy: rescan_retry_policy,
                batch_policy: BatchPolicy {
                    max_items: rescan_batch_size,
                },
            },
            force_replay_window: true,
        };

        let summary = match rescan_service
            .catchup_with_summary(None, Some(start_id), None, false, config)
            .await
        {
            Ok(summary) => summary,
            Err(err) => {
                error!(
                    event = "startup_rescan_failed",
                    error = %err,
                    "startup rescan failed"
                );
                rescan_cancel.cancel();
                return;
            }
        };

        let listener = match FirebaseListener::new(hn_api_url.clone()) {
            Ok(listener) => listener,
            Err(err) => {
                error!(
                    event = "startup_rescan_maxitem_client_failed",
                    error = %err,
                    "failed to create maxitem client"
                );
                return;
            }
        };

        if let Ok(maxitem) = listener.get_max_id().await {
            if let Some(metrics) = REALTIME_METRICS.get() {
                let lag = maxitem.saturating_sub(summary.frontier_id).max(0);
                metrics.catchup_frontier_lag.set(lag);
            }
        }

        info!(
            event = "startup_rescan_complete",
            elapsed_ms = started_at.elapsed().as_millis(),
            frontier_id = summary.frontier_id,
            target_max_id = summary.target_max_id,
            "startup rescan completed"
        );
    });

    tokio::select! {
        _ = state.shutdown_token.cancelled() => {
            info!(event = "updater_shutdown", "updater shutting down after cancellation signal");
        }
        listener_join = listener_handle => {
            state.shutdown_token.cancel();
            match listener_join {
                Ok(Ok(())) => {
                    warn!(event = "updater_listener_exited", "listener exited unexpectedly");
                }
                Ok(Err(err)) => {
                    error!(event = "updater_listener_failed", error = %err, "listener task failed");
                }
                Err(err) => {
                    error!(event = "updater_listener_join_failed", error = %err, "listener task panicked");
                }
            }
        }
        workers_join = workers_handle => {
            state.shutdown_token.cancel();
            match workers_join {
                Ok(Ok(())) => {
                    warn!(event = "updater_workers_exited", "worker supervisor exited unexpectedly");
                }
                Ok(Err(err)) => {
                    error!(event = "updater_workers_failed", error = %err, "worker supervisor failed");
                }
                Err(err) => {
                    error!(event = "updater_workers_join_failed", error = %err, "worker supervisor panicked");
                }
            }
        }
    }

    persist_handle.abort();
    shutdown_handle.abort();
    server_handle.abort();

    0
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let cli = Cli::parse();
    let code = match cli.command {
        Command::Updater(args) => run_updater(args).await,
        Command::Catchup(args) => run_catchup_once(args, "catchup").await,
    };

    if code != 0 {
        std::process::exit(code);
    }
}
