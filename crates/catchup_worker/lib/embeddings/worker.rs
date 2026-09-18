//! Batch isolation and conditional completion, independent of loop lifecycle.
use super::EmbeddingArgs;
use hn_core::{
    db::story_search::{self, DbResult, PendingStory, SearchPool},
    embeddings::{EmbeddingClient, EmbeddingError, Workload},
};
use tracing::{info, warn};

#[derive(Debug, Default)]
pub struct BatchReport {
    pub saved: usize,
    pub discarded: usize,
    pub delayed: usize,
}

/// Split only input errors. Availability/contract failures delay all unfinished
/// members of this batch and exit to the supervisor's global pause, preventing an
/// outage from hammering the endpoint with every other pending story.
pub async fn process_batch(
    pool: &SearchPool,
    client: &EmbeddingClient,
    rows: Vec<PendingStory>,
    args: &EmbeddingArgs,
) -> DbResult<BatchReport> {
    let mut waiting = vec![rows];
    let mut report = BatchReport::default();
    while let Some(batch) = waiting.pop() {
        let inputs = batch.iter().map(PendingStory::document).collect::<Vec<_>>();
        match client.embed(&inputs, Workload::Bulk).await {
            Ok(vectors) => {
                for (story, vector) in batch.iter().zip(&vectors) {
                    if story_search::finish(pool, story, Some(vector), 0).await? {
                        report.saved += 1;
                    } else {
                        report.discarded += 1;
                    }
                }
            }
            Err(EmbeddingError::InvalidInput(_)) if batch.len() > 1 => {
                let midpoint = batch.len() / 2;
                waiting.push(batch[midpoint..].to_vec());
                waiting.push(batch[..midpoint].to_vec());
            }
            Err(err @ EmbeddingError::InvalidInput(_)) => {
                let story = &batch[0];
                warn!(event="embedding_invalid_document", story_id=story.story_id, error=%err);
                if story_search::finish(
                    pool,
                    story,
                    None,
                    args.embedding_invalid_retry_seconds as i32,
                )
                .await?
                {
                    report.delayed += 1;
                }
            }
            Err(err) => {
                let delay = match &err {
                    EmbeddingError::Http {
                        retry_after: Some(delay),
                        ..
                    } => (*delay)
                        .max(u64::from(args.embedding_retry_seconds))
                        .min(i32::MAX as u64) as i32,
                    _ => args.embedding_retry_seconds as i32,
                };
                waiting.push(batch);
                for story in waiting.iter().flatten() {
                    story_search::finish(pool, story, None, delay).await?;
                }
                warn!(event="embedding_batch_failed", error=%err, retry_seconds=delay);
                return Err(Box::new(err));
            }
        }
    }
    info!(
        event = "embedding_batch_finished",
        saved = report.saved,
        discarded = report.discarded,
        delayed = report.delayed
    );
    Ok(report)
}
