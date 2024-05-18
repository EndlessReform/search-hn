use axum::{routing::get, Router};
use backend_lib::{config::Config, firebase_listener::FirebaseListener, sync_service::SyncService};
use flume;
use std::time::Instant;

use clap::Parser;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use dotenv::dotenv;
use log::{debug, info};
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[clap(about = "Backend server for instruct-hn")]
struct Cli {
    #[clap(short, long)]
    /// Disable catchup on previous data
    no_catchup: bool,

    #[clap(short, long)]
    /// Listen for HN updates and persist them to DB
    realtime: bool,

    #[clap(long)]
    /// Start catch-up from this ID
    catchup_start: Option<i64>,

    #[clap(long)]
    /// Max number of records to catch up
    catchup_amt: Option<i64>,
}

// Health endpoint handler
async fn health_handler() -> String {
    "Healthy".to_string()
}

#[tokio::main]
async fn main() {
    info!("Starting embedding backend");
    dotenv().ok();

    let config = Config::from_env().expect("Config incorrectly specified");
    env_logger::init();
    let args = Cli::parse();
    debug!("Config loaded");

    let pool_config =
        AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&config.db_url);
    let pool = Pool::builder(pool_config)
        .build()
        .expect("Could not establish connection!");

    let shutdown_token = CancellationToken::new();
    // TODO profile this constant
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

    let (sender, receiver) = flume::unbounded::<i64>();
    let listener_cancel_token = shutdown_token.clone();
    let hn_updates_handle = tokio::spawn(async move {
        FirebaseListener::new(config.hn_api_url.clone())
            .unwrap()
            .listen_to_updates(sender, listener_cancel_token)
            .await
            .expect("HN update producer has failed!");
    });

    // TODO make this number less arbitrary
    let n_update_workers = 32;
    let update_orchestrator_handle = tokio::spawn(async move {
        sync_service
            .realtime_update(n_update_workers, receiver)
            .await
            .expect("HN update consumer has failed!");
    });

    /*
    // let text: &str = "When I was a young boy, my father took me into the city to see a marching band";
    let embedder = E5Embedder::new(&config.triton_server_addr)
        .await
        .expect("Cannot connect to Triton!");

    debug!("Embedder initialized");
    let embedding = embedder.encode(text).await.expect("Embedding failed!");
    println!("{:?}", embedding); */

    let app = Router::new()
        .route("/", get(|| async { "Hello, world!" }))
        .route("/health", get(health_handler));
    let server_handle = tokio::spawn(async move {
        axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => {
            info!("SIGTERM received, shutting down.");
        }
        _ = sigint.recv() => {
            info!("SIGINT received, shutting down.");
        }
    }

    // Trigger the shutdown
    shutdown_token.cancel();
    // Wait for all tasks to complete
    hn_updates_handle.await.unwrap();
    update_orchestrator_handle.await.unwrap();
    server_handle.abort();
}
