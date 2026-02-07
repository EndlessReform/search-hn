use catchup_worker_lib::{
    cli::parse_args, config::Config, db::build_db_pool, firebase_listener::FirebaseListener,
    server::setup_server, state::AppState, sync_service::SyncService,
};
use diesel::{pg::PgConnection, Connection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use dotenv::dotenv;
use log::{debug, info};
use tokio::signal::unix::{signal, SignalKind};
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
async fn main() {
    info!("Starting catchup worker");
    dotenv().ok();

    let config = Config::from_env().expect("Config incorrectly specified");
    env_logger::init();
    let args = parse_args();
    debug!("Config loaded");

    let mut temp_conn = PgConnection::establish(&config.db_url).unwrap();
    run_initial_migrations(&mut temp_conn).unwrap();

    // TODO: Don't swallow errors here
    let pool = build_db_pool(&config.db_url)
        .await
        .expect("Could not initialize DB pool!");

    let state = Arc::new(AppState::new(pool.clone(), CancellationToken::new()));
    let shutdown_handle = tokio::spawn(handle_shutdown_signals(state.clone()));

    let server_handle = setup_server(state.clone()).await;

    // TODO make n_workers less arbitrary
    let sync_service = SyncService::new(config.hn_api_url.clone(), pool.clone(), 200);
    if !args.no_catchup {
        let start_time = Instant::now();
        info!("Beginning catchup");
        sync_service
            .catchup(args.catchup_amt, args.catchup_start)
            .await
            .expect("Catchup failed");
        let elapsed_time = start_time.elapsed();
        info!("Catchup time elapsed: {:?}", elapsed_time);
    } else {
        info!("Skipping catchup");
    }

    if !args.realtime {
        debug!("Realtime mode isn't enabled. Exiting after catchup");
    } else {
        let (sender, receiver) = flume::unbounded::<i64>();
        let listener_cancel_token = state.shutdown_token.clone();
        let hn_updates_handle = tokio::spawn(async move {
            FirebaseListener::new(config.hn_api_url.clone())
                .unwrap()
                .listen_to_updates(sender, listener_cancel_token)
                .await
                .expect("HN update producer has failed!");
        });

        // TODO update_workers should be a config option
        let n_update_workers = 32;
        let update_orchestrator_handle = tokio::spawn(async move {
            sync_service
                .realtime_update(n_update_workers, receiver)
                .await
                .expect("HN update consumer has failed!");
        });

        // Wait for all tasks to complete
        hn_updates_handle.await.unwrap();
        update_orchestrator_handle.await.unwrap();
    }

    shutdown_handle.await.unwrap();
    server_handle.abort();
}
