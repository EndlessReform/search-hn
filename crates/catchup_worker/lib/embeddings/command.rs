//! Bounded, rerunnable historical admission and embedding in the existing binary.
use super::{process_batch, EmbeddingArgs};
use clap::Args;
use hn_core::db::{
    build_db_pool,
    story_search::{self, DbResult},
};
use tracing::info;

#[derive(Args, Debug)]
pub struct BackfillArgs {
    #[arg(long)]
    pub database_url: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub start_id: i64,
    #[arg(long, default_value_t = i64::MAX)]
    pub end_id: i64,
    #[arg(long, default_value_t = 1000)]
    pub source_chunk_size: i64,
    /// Populate pending work only; use this when the updater's embedding loop is active.
    #[arg(long)]
    pub seed_only: bool,
    #[command(flatten)]
    pub embeddings: EmbeddingArgs,
}

/// No migrations or Firebase calls. Seed short source chunks, then drain due work
/// in this range. Deferred invalid rows remain pending and produce exit status 3;
/// a transient/contract failure exits 1 and a rerun resumes safely.
pub async fn run(args: BackfillArgs) -> DbResult<bool> {
    args.embeddings.validate()?;
    if args.start_id < 1
        || args.end_id < args.start_id
        || !(1..=10000).contains(&args.source_chunk_size)
    {
        return Err("positive ordered ID range and source chunk size 1..10000 required".into());
    }
    let database_url = args
        .database_url
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .ok_or("DATABASE_URL is required")?;
    let pool = build_db_pool(&database_url, 2).await?;
    // Validate the client/recipe before making even a derived-data write.
    let client = if args.seed_only {
        story_search::recipe(&pool).await?;
        None
    } else {
        Some(args.embeddings.client(&pool).await?)
    };
    let mut cursor = args.start_id - 1;
    while let Some(last) =
        story_search::seed_chunk(&pool, cursor, args.end_id, args.source_chunk_size).await?
    {
        cursor = last;
        info!(
            event = "embedding_backfill_seed",
            cursor,
            end_id = args.end_id
        );
    }
    if let Some(client) = client {
        loop {
            let rows = story_search::due(
                &pool,
                args.embeddings.embedding_batch_size as i64,
                args.start_id,
                args.end_id,
            )
            .await?;
            if rows.is_empty() {
                break;
            }
            process_batch(&pool, &client, rows, &args.embeddings).await?;
        }
    }
    let counts = story_search::counts(&pool, args.start_id, args.end_id).await?;
    info!(
        event = "embedding_backfill_finished",
        pending = counts.pending,
        due = counts.due,
        seed_only = args.seed_only
    );
    Ok(args.seed_only || counts.pending == 0)
}
