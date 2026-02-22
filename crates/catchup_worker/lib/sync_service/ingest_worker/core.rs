use tracing::warn;

use crate::db::models;
use hn_core::HnItem;

use super::super::types::{
    FetchDiagnostics, FetchItemResponse, IngestWorkerConfig, ItemOutcome, ItemOutcomeKind,
    RetryPolicy, FAILURE_CLASS_NETWORK_TRANSIENT, FAILURE_CLASS_SCHEMA,
};
use super::fetcher::ItemFetcher;
use super::persister::{dedupe_items_by_id, dedupe_kids_by_edge, BatchPersister};
use super::retry::run_with_retry;

pub(crate) enum FetchAttemptResult {
    Found {
        item: models::Item,
        kids: Vec<models::Kid>,
    },
    TerminalMissing {
        attempts: u32,
    },
    RetryableFailure {
        attempts: u32,
        failure_class: String,
        message: String,
        diagnostics: Option<FetchDiagnostics>,
    },
    FatalFailure {
        attempts: u32,
        failure_class: String,
        message: String,
        diagnostics: Option<FetchDiagnostics>,
    },
}

/// Shared micro-level ingestion logic used by both segment and realtime executors.
///
/// This owns item fetch retry/classification plus batch flush retry/classification.
/// Scheduler-specific behavior (segment cursor orchestration vs realtime queue drain) lives in
/// dedicated executors.
pub(crate) struct IngestCore<F, P>
where
    F: ItemFetcher,
    P: BatchPersister,
{
    fetcher: F,
    persister: P,
    config: IngestWorkerConfig,
}

impl<F, P> IngestCore<F, P>
where
    F: ItemFetcher,
    P: BatchPersister,
{
    pub(crate) fn new(fetcher: F, persister: P, config: IngestWorkerConfig) -> Self {
        Self {
            fetcher,
            persister,
            config,
        }
    }

    pub(crate) fn config(&self) -> IngestWorkerConfig {
        self.config
    }

    pub(crate) async fn fetch_with_retry(&self, item_id: i64) -> FetchAttemptResult {
        match run_with_retry(
            &self.config.retry_policy,
            item_id,
            |_| self.fetcher.fetch_item(item_id),
            |err| err.is_retryable(),
        )
        .await
        {
            Ok((FetchItemResponse::Found(raw_item), _attempts)) => {
                let (item, kids) = convert_item(raw_item);
                FetchAttemptResult::Found { item, kids }
            }
            Ok((FetchItemResponse::Missing, attempts)) => {
                FetchAttemptResult::TerminalMissing { attempts }
            }
            Err(terminal) if terminal.exhausted_retryable => FetchAttemptResult::RetryableFailure {
                attempts: terminal.attempts,
                failure_class: terminal.error.failure_class,
                message: terminal.error.message,
                diagnostics: terminal.error.diagnostics,
            },
            Err(terminal) => FetchAttemptResult::FatalFailure {
                attempts: terminal.attempts,
                failure_class: terminal.error.failure_class,
                message: terminal.error.message,
                diagnostics: terminal.error.diagnostics,
            },
        }
    }

    pub(crate) async fn flush_batch_with_retry(
        &self,
        items_batch: &mut Vec<models::Item>,
        kids_batch: &mut Vec<models::Kid>,
        item_id_for_error: i64,
        retry_policy: &RetryPolicy,
    ) -> Option<ItemOutcome> {
        if items_batch.is_empty() {
            if !kids_batch.is_empty() {
                return Some(ItemOutcome {
                    item_id: item_id_for_error,
                    kind: ItemOutcomeKind::FatalFailure,
                    attempts: 0,
                    failure_class: Some(FAILURE_CLASS_SCHEMA.to_string()),
                    message: Some(
                        "internal invariant violated: non-empty kids batch with empty items batch"
                            .to_string(),
                    ),
                    diagnostics: None,
                });
            }
            return None;
        }

        let duplicate_item_rows = dedupe_items_by_id(items_batch);
        let duplicate_kid_rows = dedupe_kids_by_edge(kids_batch);
        if duplicate_item_rows > 0 || duplicate_kid_rows > 0 {
            warn!(
                event = "ingest_batch_deduplicated",
                item_id_for_error,
                duplicate_item_rows,
                duplicate_kid_rows,
                unique_item_rows = items_batch.len(),
                unique_kid_rows = kids_batch.len(),
                "deduplicated conflicting rows before batch upsert"
            );
        }

        match run_with_retry(
            retry_policy,
            item_id_for_error,
            |_| self.persister.persist_batch(items_batch, kids_batch),
            |err| err.is_retryable(),
        )
        .await
        {
            Ok(((), _attempts)) => {
                items_batch.clear();
                kids_batch.clear();
                None
            }
            Err(terminal) if terminal.exhausted_retryable => Some(ItemOutcome {
                item_id: item_id_for_error,
                kind: ItemOutcomeKind::RetryableFailure,
                attempts: terminal.attempts,
                failure_class: Some(FAILURE_CLASS_NETWORK_TRANSIENT.to_string()),
                message: Some(terminal.error.message),
                diagnostics: None,
            }),
            Err(terminal) => Some(ItemOutcome {
                item_id: item_id_for_error,
                kind: ItemOutcomeKind::FatalFailure,
                attempts: terminal.attempts,
                failure_class: Some(FAILURE_CLASS_SCHEMA.to_string()),
                message: Some(terminal.error.message),
                diagnostics: None,
            }),
        }
    }

    pub(crate) fn effective_batch_size(&self) -> usize {
        self.config.batch_policy.max_items.max(1)
    }
}

fn convert_item(raw_item: HnItem) -> (models::Item, Vec<models::Kid>) {
    let mut kids_rows = Vec::new();
    if let Some(kids) = &raw_item.kids {
        for (idx, kid) in kids.iter().enumerate() {
            kids_rows.push(models::Kid {
                item: raw_item.id,
                kid: *kid,
                display_order: Some(idx as i64),
            });
        }
    }

    let mut item = models::Item::from(raw_item);
    let sanitized_fields = item.strip_embedded_nul_bytes();
    if !sanitized_fields.is_empty() {
        warn!(
            event = "item_payload_sanitized",
            item_id = item.id,
            sanitized_fields = ?sanitized_fields,
            "removed embedded NUL bytes from item payload text fields before Postgres write"
        );
    }

    (item, kids_rows)
}
