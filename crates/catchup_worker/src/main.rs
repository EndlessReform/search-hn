use catchup_worker_lib::{
    cli::parse_args, config::Config, db::build_db_pool, firebase_listener::FirebaseListener,
    logging::init_logging, server::setup_server, state::AppState, sync_service::SyncService,
};
use std::sync::Arc;
use std::time::Instant;

use dotenv::dotenv;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, Instrument};

/// Default catchup workers for service mode.
///
/// With a global 250 RPS limiter and ~50ms median item fetch latency, steady-state concurrency
/// demand is roughly 13 in-flight requests (`250 * 0.05`). We keep a modest buffer above that
/// (roughly ~1.8x) to absorb jitter and DB flush pauses without over-spawning tasks.
const DEFAULT_CATCHUP_WORKERS: usize = 24;

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

#[tokio::main]
async fn main() {
    dotenv().ok();
    let args = parse_args();

    let logging_context = init_logging("catchup_worker", "service", "info");
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
    info!(event = "service_starting", "starting catchup worker");

    let config = Config::from_env().expect("Config incorrectly specified");
    debug!(event = "config_loaded", "loaded runtime configuration");

    // TODO: Don't swallow errors here
    let pool = build_db_pool(&config.db_url)
        .await
        .expect("Could not initialize DB pool!");

    let state = Arc::new(AppState::new(pool.clone(), CancellationToken::new()));
    let shutdown_handle = tokio::spawn(handle_shutdown_signals(state.clone()).in_current_span());

    let server_handle = setup_server(state.clone()).await;

    let sync_service = SyncService::new(
        config.hn_api_url.clone(),
        pool.clone(),
        logging_context.run_id.clone(),
        DEFAULT_CATCHUP_WORKERS,
    );
    if !args.no_catchup {
        let start_time = Instant::now();
        info!(
            event = "catchup_start",
            catchup_limit = ?args.catchup_amt,
            catchup_start = ?args.catchup_start,
            "beginning catchup"
        );
        sync_service
            .catchup(args.catchup_amt, args.catchup_start)
            .await
            .expect("Catchup failed");
        let elapsed_time = start_time.elapsed();
        info!(
            event = "catchup_complete",
            elapsed_ms = elapsed_time.as_millis(),
            "catchup completed"
        );
    } else {
        info!(event = "catchup_skipped", "skipping catchup");
    }

    if !args.realtime {
        debug!(
            event = "realtime_disabled",
            "realtime mode isn't enabled; exiting after catchup"
        );
    } else {
        let (sender, receiver) = flume::unbounded::<i64>();
        let listener_cancel_token = state.shutdown_token.clone();
        let hn_updates_handle = tokio::spawn(
            async move {
                FirebaseListener::new(config.hn_api_url.clone())
                    .unwrap()
                    .listen_to_updates(sender, listener_cancel_token)
                    .await
                    .expect("HN update producer has failed!");
            }
            .in_current_span(),
        );

        // TODO update_workers should be a config option
        let n_update_workers = 32;
        let update_orchestrator_handle = tokio::spawn(
            async move {
                sync_service
                    .realtime_update(n_update_workers, receiver)
                    .await
                    .expect("HN update consumer has failed!");
            }
            .in_current_span(),
        );

        // Wait for all tasks to complete
        hn_updates_handle.await.unwrap();
        update_orchestrator_handle.await.unwrap();
    }

    shutdown_handle.await.unwrap();
    server_handle.abort();
}
