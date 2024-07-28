use backend_lib::cli::{parse_args, Mode};
use backend_lib::worker::pool::WorkerPool;
use backend_lib::{
    catchup::CatchupService, config::Config, db::build_db_pool, firebase_client::FirebaseListener,
    queue::RangesQueue, server::setup_server, state::AppState,
};
use diesel::{pg::PgConnection, Connection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};

use dotenv::dotenv;
use log::{debug, info};
use tokio::signal::unix::{signal, SignalKind};
use tokio::task;
use tokio_util::sync::CancellationToken;

/// Gracefully shuts down the application when a SIGTERM or SIGINT signal is received.
async fn handle_shutdown_signals(state: Arc<AppState>) {
    let mut sigterm =
        signal(SignalKind::terminate()).expect("Failed to register SIGTERM signal handler");
    let mut sigint =
        signal(SignalKind::interrupt()).expect("Failed to register SIGINT signal handler");

    tokio::select! {
        _ = sigterm.recv() => {
            info!("SIGTERM received, shutting down.");
        }
        _ = sigint.recv() => {
            info!("SIGINT received, shutting down.");
        }
    }

    state.shutdown_token.cancel();
}

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

fn run_initial_migrations(
    connection: &mut impl MigrationHarness<diesel::pg::Pg>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    connection.run_pending_migrations(MIGRATIONS)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    info!("Starting crawler backend");
    dotenv().ok();

    let config = Config::from_env()?;
    let args = parse_args();
    debug!("Config loaded");
    debug!("Running in {:?} mode", &args.mode);

    // Startup logic
    let mut temp_conn = PgConnection::establish(&config.db_url).map_err(|e| {
        eprintln!("Failed to initialize database: {}", e);
        e
    })?;
    run_initial_migrations(&mut temp_conn).unwrap();

    // TODO: Don't swallow errors here
    let pool = build_db_pool(&config.db_url)
        .await
        .expect("Could not initialize DB pool!");
    let leader_queue = Arc::new(RangesQueue::new(&config.redis_url, "hn_ranges").await?);

    let state = Arc::new(AppState::new(pool.clone(), CancellationToken::new()));
    let shutdown_handle = tokio::spawn(handle_shutdown_signals(state.clone()));

    /*
    START PROCESSES
    */
    let server_handle = setup_server(state.clone()).await;
    // Start workers unless leader-only
    let catchup_handle = if args.mode != Mode::Worker && !args.no_catchup {
        let url: String = config.hn_api_url.clone();
        let catchup_queue = leader_queue.clone();
        let catchup_pool = pool.clone();
        Some(task::spawn(async move {
            let sync_service =
                CatchupService::new(url, catchup_pool, catchup_queue, config.n_workers);
            let start_time = Instant::now();
            info!("Beginning catchup");
            if let Err(e) = sync_service
                .catchup(args.catchup_amt, args.catchup_start)
                .await
            {
                log::error!("Catchup failed: {:?}", e);
            }
            let elapsed_time = start_time.elapsed();
            info!("Catchup time elapsed: {:?}", elapsed_time);
        }))
    } else {
        info!("Skipping catchup");
        None
    };

    let hn_listener_handle = match args.mode {
        Mode::Worker => None,
        _ => {
            if !args.realtime {
                debug!("Realtime mode isn't enabled. Exiting after catchup");
                None
            } else {
                let listener_cancel_token = state.shutdown_token.clone();
                let hn_url = config.hn_api_url.clone();
                let listener_queue = leader_queue.clone();
                let hn_updates_handle = tokio::spawn(async move {
                    FirebaseListener::new(hn_url)
                        .unwrap()
                        .listen_to_updates(listener_queue, listener_cancel_token)
                        .await
                        .expect("HN update producer has failed!");
                });
                info!("Listener up");
                Some(hn_updates_handle)
            }
        }
    };

    let worker_queue = Arc::new(RangesQueue::new(&config.redis_url, "hn_ranges").await?);
    let worker_pool = match args.mode {
        Mode::Leader => None,
        _ => {
            debug!("Waiting to initialize workers until some work is in the system");
            sleep(Duration::from_secs(3)).await;
            Some(WorkerPool::new(
                config.n_workers,
                &config.hn_api_url,
                pool.clone(),
                worker_queue.clone(),
                state.shutdown_token.clone(),
            ))
        }
    };

    // Shutdown logic
    shutdown_handle.await.unwrap();
    if let Some(handle) = catchup_handle {
        handle.await.expect("Catchup task panicked");
    }
    if let Some(handle) = hn_listener_handle {
        handle.await.unwrap();
    };
    if let Some(pool) = worker_pool {
        pool.wait_for_completion().await.unwrap();
    }
    server_handle.abort();
    info!("Shutdown complete!");

    // Wait for all tasks to complete
    Ok(())
}
