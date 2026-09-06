//! One supervised embedding loop inside the updater. PostgreSQL is the work list;
//! HTTP failures leave it resumable, and ingestion never waits for inference.
pub mod command;
mod worker;
pub use worker::{process_batch, BatchReport};

use clap::Args;
use hn_core::{
    db::story_search::{self, DbResult, SearchPool},
    embeddings::EmbeddingClient,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Shared knobs for the service and one-shot backfill. Small batches respect the
/// shared proxy's eight-input contract and keep background GPU operations short.
#[derive(Args, Debug, Clone)]
pub struct EmbeddingArgs {
    /// Proxy API base, e.g. https://host/embeddings/v1; or EMBEDDING_BASE_URL.
    #[arg(long)]
    pub embedding_base_url: Option<String>,
    #[arg(long, default_value_t = 4)]
    pub embedding_batch_size: usize,
    #[arg(long, default_value_t = 5)]
    pub embedding_poll_seconds: u64,
    #[arg(long, default_value_t = 60)]
    pub embedding_retry_seconds: u32,
    #[arg(long, default_value_t = 3600)]
    pub embedding_invalid_retry_seconds: u32,
    #[arg(long, default_value_t = 40)]
    pub embedding_timeout_seconds: u64,
}

impl EmbeddingArgs {
    pub fn base_url(&self) -> Option<String> {
        self.embedding_base_url
            .clone()
            .or_else(|| std::env::var("EMBEDDING_BASE_URL").ok())
    }

    pub fn validate(&self) -> DbResult<()> {
        if !(1..=8).contains(&self.embedding_batch_size)
            || self.embedding_poll_seconds == 0
            || self.embedding_timeout_seconds == 0
            || self.embedding_retry_seconds == 0
            || self.embedding_retry_seconds > i32::MAX as u32
            || self.embedding_invalid_retry_seconds == 0
            || self.embedding_invalid_retry_seconds > i32::MAX as u32
        {
            return Err("embedding batch size must be 1..8; timeouts/delays positive; retry delays <= i32::MAX".into());
        }
        Ok(())
    }

    pub async fn client(&self, pool: &SearchPool) -> DbResult<EmbeddingClient> {
        self.validate()?;
        let base = self.base_url().ok_or("embedding base URL is required")?;
        Ok(EmbeddingClient::new(
            &base,
            story_search::recipe(pool).await?,
            Duration::from_secs(self.embedding_timeout_seconds),
        )?)
    }
}

/// Spawn only when explicitly configured. The supervisor catches both returned
/// errors and panics, waits, then recreates the loop. Its failures do not cancel
/// the updater's shared shutdown token. Shutdown aborts the child, releasing HTTP
/// and database resources while unfinished work remains NULL in PostgreSQL.
pub fn supervise(
    pool: SearchPool,
    args: EmbeddingArgs,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut child = tokio::spawn(run_loop(pool.clone(), args.clone()));
            let pause = tokio::select! {
                _ = cancel.cancelled() => { child.abort(); let _ = child.await; return; }
                result = &mut child => {
                    let mut pause = u64::from(args.embedding_retry_seconds).max(1);
                    if let Ok(Err(err)) = &result {
                        if let Some(hn_core::embeddings::EmbeddingError::Http { retry_after: Some(seconds), .. }) = err.downcast_ref() {
                            pause = pause.max(*seconds);
                        }
                    }
                    error!(event="embedding_loop_exited", ?result, pause, "embedding loop will restart; ingestion continues");
                    pause
                },
            };
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(pause)) => {}
            }
        }
    })
}

async fn run_loop(pool: SearchPool, args: EmbeddingArgs) -> DbResult<()> {
    let client = args.client(&pool).await?;
    let mut report_at = tokio::time::Instant::now();
    loop {
        let rows = story_search::due(&pool, args.embedding_batch_size as i64, 1, i64::MAX).await?;
        let empty = rows.is_empty();
        if !empty {
            process_batch(&pool, &client, rows, &args).await?;
        }
        if report_at.elapsed() >= Duration::from_secs(60) {
            let counts = story_search::counts(&pool, 1, i64::MAX).await?;
            info!(
                event = "embedding_progress",
                pending = counts.pending,
                due = counts.due
            );
            report_at = tokio::time::Instant::now();
        }
        if empty {
            tokio::time::sleep(Duration::from_secs(args.embedding_poll_seconds)).await;
        }
    }
}
