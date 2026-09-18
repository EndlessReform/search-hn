//! Standalone HTTP adapter for the pinned Pplx embedding recipe.
use clap::Parser;
use embedding_proxy::{Config, app};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = Config::parse();
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(address = %config.listen, "embedding proxy listening");
    axum::serve(listener, app(config)?)
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

/// Compose sends SIGTERM. Stop accepting requests and let bounded active calls finish.
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("install shutdown handler");
}
