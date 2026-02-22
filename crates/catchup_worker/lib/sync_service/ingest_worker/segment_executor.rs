use crate::db::models;

use super::super::types::{
    IngestWorkerConfig, ItemOutcome, ItemOutcomeKind, SegmentAttemptResult, SegmentAttemptStatus,
    SegmentWork, FAILURE_CLASS_SCHEMA,
};
use super::core::{FetchAttemptResult, IngestCore};
use super::fetcher::ItemFetcher;
use super::persister::BatchPersister;

/// Segment-oriented executor for catchup orchestration.
///
/// This handles range walking, cursor advancement, terminal-missing aggregation, and segment
/// attempt status reporting. It delegates micro retry/fetch/persist behavior to `IngestCore`.
pub struct SegmentIngestExecutor<F, P>
where
    F: ItemFetcher,
    P: BatchPersister,
{
    core: IngestCore<F, P>,
}

impl<F, P> SegmentIngestExecutor<F, P>
where
    F: ItemFetcher,
    P: BatchPersister,
{
    pub fn new(fetcher: F, persister: P, config: IngestWorkerConfig) -> Self {
        Self {
            core: IngestCore::new(fetcher, persister, config),
        }
    }

    /// Executes one segment-processing attempt.
    ///
    /// Returned `SegmentAttemptResult` is intentionally policy-neutral so the orchestrator can
    /// make the durable control-plane transition (`done` / `retry_wait` / `dead_letter`).
    pub async fn process_segment_once(&self, work: &SegmentWork) -> SegmentAttemptResult {
        if let Err(message) = work.validate() {
            return SegmentAttemptResult {
                segment_id: work.segment_id,
                status: SegmentAttemptStatus::FatalFailure,
                last_durable_id: None,
                unresolved_count: 0,
                terminal_missing_ids: Vec::new(),
                failure: Some(ItemOutcome {
                    item_id: work.start_id,
                    kind: ItemOutcomeKind::FatalFailure,
                    attempts: 0,
                    failure_class: Some(FAILURE_CLASS_SCHEMA.to_string()),
                    message: Some(format!("invalid segment assignment: {message}")),
                    diagnostics: None,
                }),
            };
        }

        let next_id = match work.next_id() {
            Ok(value) => value,
            Err(message) => {
                return SegmentAttemptResult {
                    segment_id: work.segment_id,
                    status: SegmentAttemptStatus::FatalFailure,
                    last_durable_id: None,
                    unresolved_count: 0,
                    terminal_missing_ids: Vec::new(),
                    failure: Some(ItemOutcome {
                        item_id: work.start_id,
                        kind: ItemOutcomeKind::FatalFailure,
                        attempts: 0,
                        failure_class: Some(FAILURE_CLASS_SCHEMA.to_string()),
                        message: Some(format!("invalid segment assignment: {message}")),
                        diagnostics: None,
                    }),
                };
            }
        };

        if next_id > work.end_id {
            return SegmentAttemptResult {
                segment_id: work.segment_id,
                status: SegmentAttemptStatus::Completed,
                last_durable_id: Some(work.end_id),
                unresolved_count: 0,
                terminal_missing_ids: Vec::new(),
                failure: None,
            };
        }

        let mut items_batch: Vec<models::Item> = Vec::new();
        let mut kids_batch: Vec<models::Kid> = Vec::new();
        let mut terminal_missing_ids = Vec::new();
        let mut last_durable_id = work
            .resume_cursor_id
            .filter(|cursor| *cursor >= work.start_id);

        for item_id in next_id..=work.end_id {
            match self.core.fetch_with_retry(item_id).await {
                FetchAttemptResult::Found { item, kids } => {
                    items_batch.push(item);
                    kids_batch.extend(kids);

                    if items_batch.len() >= self.core.effective_batch_size() {
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
                            return self.segment_failure_result(
                                work,
                                last_durable_id,
                                failure,
                                terminal_missing_ids,
                            );
                        }
                        last_durable_id = Some(item_id);
                    }
                }
                FetchAttemptResult::TerminalMissing { attempts: _ } => {
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
                        return self.segment_failure_result(
                            work,
                            last_durable_id,
                            failure,
                            terminal_missing_ids,
                        );
                    }

                    terminal_missing_ids.push(item_id);
                    last_durable_id = Some(item_id);
                }
                FetchAttemptResult::RetryableFailure {
                    attempts,
                    failure_class,
                    message,
                    diagnostics,
                } => {
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
                        return self.segment_failure_result(
                            work,
                            last_durable_id,
                            failure,
                            terminal_missing_ids,
                        );
                    }

                    return self.segment_failure_result(
                        work,
                        last_durable_id,
                        ItemOutcome {
                            item_id,
                            kind: ItemOutcomeKind::RetryableFailure,
                            attempts,
                            failure_class: Some(failure_class),
                            message: Some(message),
                            diagnostics,
                        },
                        terminal_missing_ids,
                    );
                }
                FetchAttemptResult::FatalFailure {
                    attempts,
                    failure_class,
                    message,
                    diagnostics,
                } => {
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
                        return self.segment_failure_result(
                            work,
                            last_durable_id,
                            failure,
                            terminal_missing_ids,
                        );
                    }

                    return self.segment_failure_result(
                        work,
                        last_durable_id,
                        ItemOutcome {
                            item_id,
                            kind: ItemOutcomeKind::FatalFailure,
                            attempts,
                            failure_class: Some(failure_class),
                            message: Some(message),
                            diagnostics,
                        },
                        terminal_missing_ids,
                    );
                }
            }
        }

        if let Some(failure) = self
            .core
            .flush_batch_with_retry(
                &mut items_batch,
                &mut kids_batch,
                work.end_id,
                &self.core.config().retry_policy,
            )
            .await
        {
            return self.segment_failure_result(
                work,
                last_durable_id,
                failure,
                terminal_missing_ids,
            );
        }

        SegmentAttemptResult {
            segment_id: work.segment_id,
            status: SegmentAttemptStatus::Completed,
            last_durable_id: Some(work.end_id),
            unresolved_count: 0,
            terminal_missing_ids,
            failure: None,
        }
    }

    fn segment_failure_result(
        &self,
        work: &SegmentWork,
        last_durable_id: Option<i64>,
        failure: ItemOutcome,
        terminal_missing_ids: Vec<i64>,
    ) -> SegmentAttemptResult {
        let status = match failure.kind {
            ItemOutcomeKind::RetryableFailure => SegmentAttemptStatus::RetryableFailure,
            _ => SegmentAttemptStatus::FatalFailure,
        };

        SegmentAttemptResult {
            segment_id: work.segment_id,
            status,
            last_durable_id,
            unresolved_count: unresolved_count(work.start_id, work.end_id, last_durable_id),
            terminal_missing_ids,
            failure: Some(failure),
        }
    }
}

fn unresolved_count(start_id: i64, end_id: i64, last_durable_id: Option<i64>) -> i32 {
    let first_unresolved = match last_durable_id {
        Some(last) => last + 1,
        None => start_id,
    };

    if first_unresolved > end_id {
        return 0;
    }

    let count_i64 = end_id - first_unresolved + 1;
    i32::try_from(count_i64).unwrap_or(i32::MAX)
}
