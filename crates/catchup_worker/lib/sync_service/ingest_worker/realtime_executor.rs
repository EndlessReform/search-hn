use super::super::types::{IngestWorkerConfig, ItemOutcome, ItemOutcomeKind, RealtimeIngestResult};
use super::core::{FetchAttemptResult, IngestCore};
use super::fetcher::ItemFetcher;
use super::persister::BatchPersister;

/// Realtime-oriented executor for single-item ingestion.
///
/// Unlike segment execution, realtime work arrives as sparse, ephemeral IDs from SSE. This
/// executor intentionally processes one ID at a time and immediately flushes any resulting write
/// batch, while still sharing the same micro fetch/persist retry behavior through `IngestCore`.
pub struct RealtimeIngestExecutor<F, P>
where
    F: ItemFetcher,
    P: BatchPersister,
{
    core: IngestCore<F, P>,
}

impl<F, P> RealtimeIngestExecutor<F, P>
where
    F: ItemFetcher,
    P: BatchPersister,
{
    pub fn new(fetcher: F, persister: P, config: IngestWorkerConfig) -> Self {
        Self {
            core: IngestCore::new(fetcher, persister, config),
        }
    }

    /// Executes one realtime item ingestion attempt.
    ///
    /// The scheduler/supervisor decides whether to drop, dead-letter, or replay failures.
    /// This method only returns normalized per-item outcomes.
    pub async fn process_item_once(&self, item_id: i64) -> RealtimeIngestResult {
        match self.core.fetch_with_retry(item_id).await {
            FetchAttemptResult::Found { item, kids } => {
                let mut items_batch = vec![item];
                let mut kids_batch = kids;
                if let Some(failure) = self
                    .core
                    .flush_batch_with_retry(
                        &mut items_batch,
                        &mut kids_batch,
                        item_id,
                        &self.core.config().retry_policy,
                    )
                    .await
                {
                    return RealtimeIngestResult { outcome: failure };
                }

                RealtimeIngestResult {
                    outcome: ItemOutcome {
                        item_id,
                        kind: ItemOutcomeKind::Succeeded,
                        attempts: 1,
                        failure_class: None,
                        message: None,
                        diagnostics: None,
                    },
                }
            }
            FetchAttemptResult::TerminalMissing { attempts } => RealtimeIngestResult {
                outcome: ItemOutcome {
                    item_id,
                    kind: ItemOutcomeKind::TerminalMissing,
                    attempts,
                    failure_class: None,
                    message: None,
                    diagnostics: None,
                },
            },
            FetchAttemptResult::RetryableFailure {
                attempts,
                failure_class,
                message,
                diagnostics,
            } => RealtimeIngestResult {
                outcome: ItemOutcome {
                    item_id,
                    kind: ItemOutcomeKind::RetryableFailure,
                    attempts,
                    failure_class: Some(failure_class),
                    message: Some(message),
                    diagnostics,
                },
            },
            FetchAttemptResult::FatalFailure {
                attempts,
                failure_class,
                message,
                diagnostics,
            } => RealtimeIngestResult {
                outcome: ItemOutcome {
                    item_id,
                    kind: ItemOutcomeKind::FatalFailure,
                    attempts,
                    failure_class: Some(failure_class),
                    message: Some(message),
                    diagnostics,
                },
            },
        }
    }
}
