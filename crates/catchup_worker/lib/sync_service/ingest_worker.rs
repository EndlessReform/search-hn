use std::sync::Arc;
use std::time::Duration;

use diesel::insert_into;
use diesel::pg::upsert::excluded;
use diesel::prelude::*;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::RunQueryDsl;
use futures::future::BoxFuture;

use crate::db::models;
use crate::db::schema::{items, kids};
use crate::firebase_listener::{FirebaseListener, FirebaseListenerErr};

use super::types::{
    FetchError, FetchErrorKind, FetchItemResponse, IngestWorkerConfig, ItemOutcome,
    ItemOutcomeKind, PersistError, RealtimeIngestResult, RetryPolicy, SegmentAttemptResult,
    SegmentAttemptStatus, SegmentWork,
};

/// Fetches one HN item by ID.
///
/// This trait exists so worker logic can be unit-tested against deterministic scripted failures
/// without requiring live network access.
pub trait ItemFetcher: Send + Sync {
    fn fetch_item<'a>(
        &'a self,
        item_id: i64,
    ) -> BoxFuture<'a, Result<FetchItemResponse, FetchError>>;
}

/// Persists a batch of item rows and item->kid edges.
///
/// This is intentionally abstracted so we can test transient/fatal persistence behavior without
/// requiring a Postgres instance.
pub trait BatchPersister: Send + Sync {
    fn persist_batch<'a>(
        &'a self,
        items_batch: &'a [models::Item],
        kids_batch: &'a [models::Kid],
    ) -> BoxFuture<'a, Result<(), PersistError>>;
}

impl<T> ItemFetcher for Arc<T>
where
    T: ItemFetcher + ?Sized,
{
    fn fetch_item<'a>(
        &'a self,
        item_id: i64,
    ) -> BoxFuture<'a, Result<FetchItemResponse, FetchError>> {
        (**self).fetch_item(item_id)
    }
}

impl<T> BatchPersister for Arc<T>
where
    T: BatchPersister + ?Sized,
{
    fn persist_batch<'a>(
        &'a self,
        items_batch: &'a [models::Item],
        kids_batch: &'a [models::Kid],
    ) -> BoxFuture<'a, Result<(), PersistError>> {
        (**self).persist_batch(items_batch, kids_batch)
    }
}

/// Firebase-backed fetcher implementation used by production runtime.
pub struct FirebaseItemFetcher {
    listener: FirebaseListener,
}

impl FirebaseItemFetcher {
    pub fn new(firebase_url: String) -> Result<Self, FirebaseListenerErr> {
        Ok(Self {
            listener: FirebaseListener::new(firebase_url)?,
        })
    }
}

impl ItemFetcher for FirebaseItemFetcher {
    fn fetch_item<'a>(
        &'a self,
        item_id: i64,
    ) -> BoxFuture<'a, Result<FetchItemResponse, FetchError>> {
        Box::pin(async move {
            match self.listener.get_item(item_id).await {
                Ok(Some(item)) => Ok(FetchItemResponse::Found(item)),
                Ok(None) => Ok(FetchItemResponse::Missing),
                Err(err) => Err(map_firebase_error(err)),
            }
        })
    }
}

/// Postgres-backed batch persister used by production runtime.
pub struct PgBatchPersister {
    pool: Pool<diesel_async::AsyncPgConnection>,
}

impl PgBatchPersister {
    pub fn new(pool: Pool<diesel_async::AsyncPgConnection>) -> Self {
        Self { pool }
    }
}

impl BatchPersister for PgBatchPersister {
    fn persist_batch<'a>(
        &'a self,
        items_batch: &'a [models::Item],
        kids_batch: &'a [models::Kid],
    ) -> BoxFuture<'a, Result<(), PersistError>> {
        Box::pin(async move {
            if items_batch.is_empty() {
                if kids_batch.is_empty() {
                    return Ok(());
                }
                return Err(PersistError::fatal(
                    "internal invariant violated: non-empty kids batch with empty items batch",
                ));
            }

            let mut conn = self.pool.get().await.map_err(|err| {
                PersistError::retryable(format!("failed to acquire DB pool connection: {err}"))
            })?;

            insert_into(items::dsl::items)
                .values(items_batch)
                .on_conflict(items::id)
                .do_update()
                .set((
                    items::deleted.eq(excluded(items::deleted)),
                    items::type_.eq(excluded(items::type_)),
                    items::by.eq(excluded(items::by)),
                    items::time.eq(excluded(items::time)),
                    items::text.eq(excluded(items::text)),
                    items::dead.eq(excluded(items::dead)),
                    items::parent.eq(excluded(items::parent)),
                    items::poll.eq(excluded(items::poll)),
                    items::url.eq(excluded(items::url)),
                    items::score.eq(excluded(items::score)),
                    items::title.eq(excluded(items::title)),
                    items::parts.eq(excluded(items::parts)),
                    items::descendants.eq(excluded(items::descendants)),
                ))
                .execute(&mut conn)
                .await
                .map_err(map_diesel_error)?;

            if !kids_batch.is_empty() {
                insert_into(kids::dsl::kids)
                    .values(kids_batch)
                    .on_conflict((kids::item, kids::kid))
                    .do_update()
                    .set(kids::display_order.eq(excluded(kids::display_order)))
                    .execute(&mut conn)
                    .await
                    .map_err(map_diesel_error)?;
            }

            Ok(())
        })
    }
}

/// Stateless data-plane worker.
///
/// The worker owns fetch + micro-retry + data persistence. It does not own segment leasing,
/// dead-letter transitions, or frontier advancement. Those stay in the orchestrator.
pub struct IngestWorker<F, P>
where
    F: ItemFetcher,
    P: BatchPersister,
{
    fetcher: F,
    persister: P,
    config: IngestWorkerConfig,
}

impl<F, P> IngestWorker<F, P>
where
    F: ItemFetcher,
    P: BatchPersister,
{
    pub fn new(fetcher: F, persister: P, config: IngestWorkerConfig) -> Self {
        Self {
            fetcher,
            persister,
            config,
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
                    message: Some(format!("invalid segment assignment: {message}")),
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
                        message: Some(format!("invalid segment assignment: {message}")),
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

        let mut items_batch = Vec::new();
        let mut kids_batch = Vec::new();
        let mut terminal_missing_ids = Vec::new();
        let mut last_durable_id = work
            .resume_cursor_id
            .filter(|cursor| *cursor >= work.start_id);

        for item_id in next_id..=work.end_id {
            match self.fetch_with_retry(item_id).await {
                FetchAttemptResult::Found { item, kids } => {
                    items_batch.push(item);
                    kids_batch.extend(kids);

                    if items_batch.len() >= self.effective_batch_size() {
                        if let Some(failure) = self
                            .flush_batch_with_retry(
                                &mut items_batch,
                                &mut kids_batch,
                                item_id,
                                &self.config.retry_policy,
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
                        .flush_batch_with_retry(
                            &mut items_batch,
                            &mut kids_batch,
                            item_id,
                            &self.config.retry_policy,
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
                FetchAttemptResult::RetryableFailure { attempts, message } => {
                    if let Some(failure) = self
                        .flush_batch_with_retry(
                            &mut items_batch,
                            &mut kids_batch,
                            item_id,
                            &self.config.retry_policy,
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
                            message: Some(message),
                        },
                        terminal_missing_ids,
                    );
                }
                FetchAttemptResult::FatalFailure { attempts, message } => {
                    if let Some(failure) = self
                        .flush_batch_with_retry(
                            &mut items_batch,
                            &mut kids_batch,
                            item_id,
                            &self.config.retry_policy,
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
                            message: Some(message),
                        },
                        terminal_missing_ids,
                    );
                }
            }
        }

        if let Some(failure) = self
            .flush_batch_with_retry(
                &mut items_batch,
                &mut kids_batch,
                work.end_id,
                &self.config.retry_policy,
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

    /// Executes one realtime item ingestion attempt.
    pub async fn process_realtime_item(&self, item_id: i64) -> RealtimeIngestResult {
        match self.fetch_with_retry(item_id).await {
            FetchAttemptResult::Found { item, kids } => {
                let mut items_batch = vec![item];
                let mut kids_batch = kids;
                if let Some(failure) = self
                    .flush_batch_with_retry(
                        &mut items_batch,
                        &mut kids_batch,
                        item_id,
                        &self.config.retry_policy,
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
                        message: None,
                    },
                }
            }
            FetchAttemptResult::TerminalMissing { attempts } => RealtimeIngestResult {
                outcome: ItemOutcome {
                    item_id,
                    kind: ItemOutcomeKind::TerminalMissing,
                    attempts,
                    message: None,
                },
            },
            FetchAttemptResult::RetryableFailure { attempts, message } => RealtimeIngestResult {
                outcome: ItemOutcome {
                    item_id,
                    kind: ItemOutcomeKind::RetryableFailure,
                    attempts,
                    message: Some(message),
                },
            },
            FetchAttemptResult::FatalFailure { attempts, message } => RealtimeIngestResult {
                outcome: ItemOutcome {
                    item_id,
                    kind: ItemOutcomeKind::FatalFailure,
                    attempts,
                    message: Some(message),
                },
            },
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

    async fn fetch_with_retry(&self, item_id: i64) -> FetchAttemptResult {
        let retry_policy = self.config.retry_policy;

        let max_attempts = retry_policy.max_attempts.max(1);

        for attempt in 1..=max_attempts {
            match self.fetcher.fetch_item(item_id).await {
                Ok(FetchItemResponse::Found(raw_item)) => {
                    let (item, kids) = convert_item(raw_item);
                    return FetchAttemptResult::Found { item, kids };
                }
                Ok(FetchItemResponse::Missing) => {
                    return FetchAttemptResult::TerminalMissing { attempts: attempt };
                }
                Err(err) => {
                    if err.is_retryable() {
                        if attempt < max_attempts {
                            let delay = compute_backoff_delay(&retry_policy, attempt, item_id);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            continue;
                        }
                        return FetchAttemptResult::RetryableFailure {
                            attempts: attempt,
                            message: err.message,
                        };
                    }

                    return FetchAttemptResult::FatalFailure {
                        attempts: attempt,
                        message: err.message,
                    };
                }
            }
        }

        FetchAttemptResult::FatalFailure {
            attempts: max_attempts,
            message: "worker retry loop exited unexpectedly".to_string(),
        }
    }

    async fn flush_batch_with_retry(
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
                    message: Some(
                        "internal invariant violated: non-empty kids batch with empty items batch"
                            .to_string(),
                    ),
                });
            }
            return None;
        }

        let max_attempts = retry_policy.max_attempts.max(1);

        for attempt in 1..=max_attempts {
            match self.persister.persist_batch(items_batch, kids_batch).await {
                Ok(()) => {
                    items_batch.clear();
                    kids_batch.clear();
                    return None;
                }
                Err(err) => {
                    if err.is_retryable() {
                        if attempt < max_attempts {
                            let delay =
                                compute_backoff_delay(retry_policy, attempt, item_id_for_error);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            continue;
                        }

                        return Some(ItemOutcome {
                            item_id: item_id_for_error,
                            kind: ItemOutcomeKind::RetryableFailure,
                            attempts: attempt,
                            message: Some(err.message),
                        });
                    }

                    return Some(ItemOutcome {
                        item_id: item_id_for_error,
                        kind: ItemOutcomeKind::FatalFailure,
                        attempts: attempt,
                        message: Some(err.message),
                    });
                }
            }
        }

        Some(ItemOutcome {
            item_id: item_id_for_error,
            kind: ItemOutcomeKind::FatalFailure,
            attempts: max_attempts,
            message: Some("persistence retry loop exited unexpectedly".to_string()),
        })
    }

    fn effective_batch_size(&self) -> usize {
        self.config.batch_policy.max_items.max(1)
    }
}

enum FetchAttemptResult {
    Found {
        item: models::Item,
        kids: Vec<models::Kid>,
    },
    TerminalMissing {
        attempts: u32,
    },
    RetryableFailure {
        attempts: u32,
        message: String,
    },
    FatalFailure {
        attempts: u32,
        message: String,
    },
}

fn convert_item(
    raw_item: crate::firebase_listener::listener::Item,
) -> (models::Item, Vec<models::Kid>) {
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

    (models::Item::from(raw_item), kids_rows)
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

fn compute_backoff_delay(policy: &RetryPolicy, attempt: u32, item_id: i64) -> Duration {
    if policy.initial_backoff.is_zero() && policy.jitter.is_zero() {
        return Duration::ZERO;
    }

    let shift = u32::min(attempt.saturating_sub(1), 20);
    let exponential_ms = policy
        .initial_backoff
        .as_millis()
        .saturating_mul(1u128 << shift);
    let capped_ms = exponential_ms.min(policy.max_backoff.as_millis());

    let jitter_ms = if policy.jitter.is_zero() {
        0
    } else {
        let jitter_cap = policy.jitter.as_millis();
        deterministic_jitter(item_id, attempt, jitter_cap)
    };

    let total_ms = capped_ms.saturating_add(jitter_ms);
    Duration::from_millis(total_ms.min(u64::MAX as u128) as u64)
}

fn deterministic_jitter(item_id: i64, attempt: u32, jitter_cap: u128) -> u128 {
    if jitter_cap == 0 {
        return 0;
    }

    let mut x = (item_id as u64) ^ (attempt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;

    (x as u128) % (jitter_cap + 1)
}

fn map_status_to_fetch_error(resource: String, status: u16) -> FetchError {
    match status {
        401 => FetchError::new(
            FetchErrorKind::Unauthorized,
            format!("unauthorized while fetching {resource}"),
        ),
        403 => FetchError::new(
            FetchErrorKind::Forbidden,
            format!("forbidden while fetching {resource}"),
        ),
        404 => FetchError::new(
            FetchErrorKind::Other,
            format!("not found while fetching {resource}"),
        ),
        429 => FetchError::new(
            FetchErrorKind::RateLimited,
            format!("rate limited while fetching {resource}"),
        ),
        500..=599 => FetchError::new(
            FetchErrorKind::UpstreamUnavailable,
            format!("upstream server error {status} while fetching {resource}"),
        ),
        _ => FetchError::new(
            FetchErrorKind::Other,
            format!("unexpected HTTP status {status} while fetching {resource}"),
        ),
    }
}

fn map_firebase_error(error: FirebaseListenerErr) -> FetchError {
    match error {
        FirebaseListenerErr::UnexpectedStatus { resource, status } => {
            map_status_to_fetch_error(resource, status)
        }
        FirebaseListenerErr::RequestError(req_err) => {
            if let Some(status) = req_err.status() {
                return map_status_to_fetch_error("item".to_string(), status.as_u16());
            }

            if req_err.is_timeout() || req_err.is_connect() {
                return FetchError::new(
                    FetchErrorKind::Network,
                    format!("network error while fetching item: {req_err}"),
                );
            }

            FetchError::new(
                FetchErrorKind::MalformedResponse,
                format!("request/parse error while fetching item: {req_err}"),
            )
        }
        FirebaseListenerErr::JsonParseError(err) => FetchError::new(
            FetchErrorKind::MalformedResponse,
            format!("invalid JSON from Firebase item endpoint: {err}"),
        ),
        FirebaseListenerErr::ParseError(message) => {
            if message.contains("null") {
                return FetchError::new(
                    FetchErrorKind::Other,
                    format!("unexpected null-like payload from Firebase item endpoint: {message}"),
                );
            }
            FetchError::new(
                FetchErrorKind::MalformedResponse,
                format!("unparseable Firebase payload: {message}"),
            )
        }
        FirebaseListenerErr::ConnectError(message) => FetchError::new(
            FetchErrorKind::Network,
            format!("connection error while fetching item: {message}"),
        ),
        FirebaseListenerErr::ChannelError(err) => FetchError::new(
            FetchErrorKind::Other,
            format!("internal channel error while fetching item: {err}"),
        ),
    }
}

fn map_diesel_error(error: DieselError) -> PersistError {
    match error {
        DieselError::DatabaseError(kind, info) => match kind {
            DatabaseErrorKind::SerializationFailure
            | DatabaseErrorKind::ClosedConnection
            | DatabaseErrorKind::UnableToSendCommand => PersistError::retryable(format!(
                "transient database error ({kind:?}): {}",
                info.message()
            )),
            _ => PersistError::fatal(format!(
                "fatal database error ({kind:?}): {}",
                info.message()
            )),
        },
        DieselError::RollbackTransaction => {
            PersistError::retryable("transaction rollback requested by database".to_string())
        }
        other => PersistError::fatal(format!("fatal diesel error: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::firebase_listener::listener::Item as FirebaseItem;
    use crate::sync_service::types::BatchPolicy;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    fn test_worker_config(max_attempts: u32, batch_size: usize) -> IngestWorkerConfig {
        IngestWorkerConfig {
            retry_policy: RetryPolicy {
                max_attempts,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                jitter: Duration::ZERO,
            },
            batch_policy: BatchPolicy {
                max_items: batch_size,
            },
        }
    }

    fn sample_item(id: i64, deleted: bool, kids: Option<Vec<i64>>) -> FirebaseItem {
        FirebaseItem {
            id,
            deleted: Some(deleted),
            type_: Some("story".to_string()),
            by: Some("alice".to_string()),
            time: Some(1_700_000_000),
            text: Some("hello".to_string()),
            dead: Some(false),
            parent: None,
            poll: None,
            url: Some("https://example.com".to_string()),
            score: Some(42),
            title: Some(format!("item-{id}")),
            parts: None,
            descendants: Some(0),
            kids,
        }
    }

    #[derive(Default)]
    struct MockFetcher {
        plans: Mutex<HashMap<i64, VecDeque<Result<FetchItemResponse, FetchError>>>>,
        call_counts: Mutex<HashMap<i64, u32>>,
    }

    impl MockFetcher {
        fn with_plan(plan: Vec<(i64, Vec<Result<FetchItemResponse, FetchError>>)>) -> Self {
            let mut plans = HashMap::new();
            for (item_id, entries) in plan {
                plans.insert(item_id, entries.into_iter().collect());
            }
            Self {
                plans: Mutex::new(plans),
                call_counts: Mutex::new(HashMap::new()),
            }
        }

        fn calls_for(&self, item_id: i64) -> u32 {
            *self
                .call_counts
                .lock()
                .expect("call_count mutex poisoned")
                .get(&item_id)
                .unwrap_or(&0)
        }
    }

    impl ItemFetcher for MockFetcher {
        fn fetch_item<'a>(
            &'a self,
            item_id: i64,
        ) -> BoxFuture<'a, Result<FetchItemResponse, FetchError>> {
            Box::pin(async move {
                {
                    let mut counts = self.call_counts.lock().expect("call_count mutex poisoned");
                    *counts.entry(item_id).or_insert(0) += 1;
                }

                let mut plans = self.plans.lock().expect("plans mutex poisoned");
                let responses = plans.get_mut(&item_id).ok_or_else(|| {
                    FetchError::new(
                        FetchErrorKind::Other,
                        format!("no scripted fetch response for item {item_id}"),
                    )
                })?;

                responses.pop_front().ok_or_else(|| {
                    FetchError::new(
                        FetchErrorKind::Other,
                        format!("scripted responses exhausted for item {item_id}"),
                    )
                })?
            })
        }
    }

    #[derive(Default)]
    struct MockPersister {
        outcomes: Mutex<VecDeque<Result<(), PersistError>>>,
        calls: Mutex<u32>,
        persisted_ids: Mutex<Vec<i64>>,
    }

    impl MockPersister {
        fn with_outcomes(outcomes: Vec<Result<(), PersistError>>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                calls: Mutex::new(0),
                persisted_ids: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> u32 {
            *self.calls.lock().expect("calls mutex poisoned")
        }

        fn persisted_ids(&self) -> Vec<i64> {
            self.persisted_ids
                .lock()
                .expect("persisted_ids mutex poisoned")
                .clone()
        }
    }

    impl BatchPersister for MockPersister {
        fn persist_batch<'a>(
            &'a self,
            items_batch: &'a [models::Item],
            _kids_batch: &'a [models::Kid],
        ) -> BoxFuture<'a, Result<(), PersistError>> {
            Box::pin(async move {
                *self.calls.lock().expect("calls mutex poisoned") += 1;

                let next = self
                    .outcomes
                    .lock()
                    .expect("outcomes mutex poisoned")
                    .pop_front()
                    .unwrap_or(Ok(()));

                if next.is_ok() {
                    let mut persisted = self
                        .persisted_ids
                        .lock()
                        .expect("persisted_ids mutex poisoned");
                    persisted.extend(items_batch.iter().map(|item| item.id));
                }

                next
            })
        }
    }

    #[tokio::test]
    async fn malformed_segment_returns_fatal_failure() {
        let fetcher = MockFetcher::default();
        let persister = MockPersister::default();
        let worker = IngestWorker::new(fetcher, persister, test_worker_config(3, 2));

        let result = worker
            .process_segment_once(&SegmentWork {
                segment_id: 1,
                start_id: 10,
                end_id: 5,
                resume_cursor_id: None,
            })
            .await;

        assert_eq!(result.status, SegmentAttemptStatus::FatalFailure);
        assert!(result
            .failure
            .as_ref()
            .expect("expected failure")
            .message
            .as_ref()
            .expect("expected message")
            .contains("invalid segment assignment"));
    }

    #[tokio::test]
    async fn missing_and_deleted_items_are_treated_as_terminal() {
        let fetcher = MockFetcher::with_plan(vec![
            (
                100,
                vec![Ok(FetchItemResponse::Found(sample_item(
                    100,
                    true,
                    Some(vec![200, 201]),
                )))],
            ),
            (101, vec![Ok(FetchItemResponse::Missing)]),
            (
                102,
                vec![Ok(FetchItemResponse::Found(sample_item(102, false, None)))],
            ),
        ]);
        let persister = MockPersister::default();
        let worker = IngestWorker::new(fetcher, persister, test_worker_config(3, 2));

        let result = worker
            .process_segment_once(&SegmentWork {
                segment_id: 42,
                start_id: 100,
                end_id: 102,
                resume_cursor_id: None,
            })
            .await;

        assert_eq!(result.status, SegmentAttemptStatus::Completed);
        assert_eq!(result.unresolved_count, 0);
        assert_eq!(result.last_durable_id, Some(102));
        assert_eq!(result.terminal_missing_ids, vec![101]);
        assert!(result.failure.is_none());
    }

    #[tokio::test]
    async fn retryable_fetch_errors_retry_and_then_succeed() {
        let fetcher = MockFetcher::with_plan(vec![(
            500,
            vec![
                Err(FetchError::new(FetchErrorKind::RateLimited, "429")),
                Err(FetchError::new(FetchErrorKind::Network, "timeout")),
                Ok(FetchItemResponse::Found(sample_item(500, false, None))),
            ],
        )]);
        let fetcher = Arc::new(fetcher);
        let persister = MockPersister::default();
        let worker = IngestWorker::new(fetcher.clone(), persister, test_worker_config(5, 10));

        let result = worker
            .process_segment_once(&SegmentWork {
                segment_id: 2,
                start_id: 500,
                end_id: 500,
                resume_cursor_id: None,
            })
            .await;

        assert_eq!(result.status, SegmentAttemptStatus::Completed);
        assert_eq!(fetcher.calls_for(500), 3);
    }

    #[tokio::test]
    async fn retryable_fetch_exhaustion_returns_retryable_failure() {
        let fetcher = MockFetcher::with_plan(vec![(
            900,
            vec![
                Err(FetchError::new(FetchErrorKind::Network, "timeout-1")),
                Err(FetchError::new(FetchErrorKind::Network, "timeout-2")),
                Err(FetchError::new(FetchErrorKind::Network, "timeout-3")),
            ],
        )]);
        let worker = IngestWorker::new(fetcher, MockPersister::default(), test_worker_config(3, 5));

        let result = worker
            .process_segment_once(&SegmentWork {
                segment_id: 3,
                start_id: 900,
                end_id: 902,
                resume_cursor_id: None,
            })
            .await;

        assert_eq!(result.status, SegmentAttemptStatus::RetryableFailure);
        assert_eq!(result.last_durable_id, None);
        assert_eq!(result.unresolved_count, 3);
        let failure = result.failure.expect("expected retryable failure");
        assert_eq!(failure.item_id, 900);
        assert_eq!(failure.kind, ItemOutcomeKind::RetryableFailure);
        assert_eq!(failure.attempts, 3);
    }

    #[tokio::test]
    async fn fatal_fetch_error_does_not_retry() {
        let fetcher = MockFetcher::with_plan(vec![(
            1200,
            vec![Err(FetchError::new(
                FetchErrorKind::Unauthorized,
                "401 unauthorized",
            ))],
        )]);
        let fetcher = Arc::new(fetcher);
        let worker = IngestWorker::new(
            fetcher.clone(),
            MockPersister::default(),
            test_worker_config(6, 5),
        );

        let result = worker
            .process_segment_once(&SegmentWork {
                segment_id: 4,
                start_id: 1200,
                end_id: 1200,
                resume_cursor_id: None,
            })
            .await;

        assert_eq!(result.status, SegmentAttemptStatus::FatalFailure);
        assert_eq!(fetcher.calls_for(1200), 1);
        assert_eq!(
            result.failure.expect("expected fatal failure").kind,
            ItemOutcomeKind::FatalFailure
        );
    }

    #[tokio::test]
    async fn retryable_persistence_error_retries_and_recovers() {
        let fetcher = MockFetcher::with_plan(vec![
            (
                700,
                vec![Ok(FetchItemResponse::Found(sample_item(700, false, None)))],
            ),
            (
                701,
                vec![Ok(FetchItemResponse::Found(sample_item(701, false, None)))],
            ),
        ]);
        let persister = Arc::new(MockPersister::with_outcomes(vec![
            Err(PersistError::retryable("temporary DB connectivity")),
            Ok(()),
        ]));

        let worker = IngestWorker::new(fetcher, persister.clone(), test_worker_config(3, 2));

        let result = worker
            .process_segment_once(&SegmentWork {
                segment_id: 5,
                start_id: 700,
                end_id: 701,
                resume_cursor_id: None,
            })
            .await;

        assert_eq!(result.status, SegmentAttemptStatus::Completed);
        assert_eq!(persister.calls(), 2);
        assert_eq!(persister.persisted_ids(), vec![700, 701]);
    }

    #[tokio::test]
    async fn fatal_persistence_error_reports_fatal_failure() {
        let fetcher = MockFetcher::with_plan(vec![(
            800,
            vec![Ok(FetchItemResponse::Found(sample_item(800, false, None)))],
        )]);
        let persister = MockPersister::with_outcomes(vec![Err(PersistError::fatal(
            "column \"parts\" does not exist",
        ))]);

        let worker = IngestWorker::new(fetcher, persister, test_worker_config(2, 10));

        let result = worker
            .process_segment_once(&SegmentWork {
                segment_id: 6,
                start_id: 800,
                end_id: 800,
                resume_cursor_id: None,
            })
            .await;

        assert_eq!(result.status, SegmentAttemptStatus::FatalFailure);
        assert_eq!(result.unresolved_count, 1);
        assert_eq!(result.last_durable_id, None);
        let failure = result.failure.expect("expected fatal persistence failure");
        assert_eq!(failure.item_id, 800);
        assert_eq!(failure.kind, ItemOutcomeKind::FatalFailure);
        assert!(failure
            .message
            .expect("expected error message")
            .contains("column \"parts\" does not exist"));
    }

    #[tokio::test]
    async fn realtime_item_missing_is_reported_as_terminal() {
        let fetcher = MockFetcher::with_plan(vec![(1500, vec![Ok(FetchItemResponse::Missing)])]);
        let worker = IngestWorker::new(fetcher, MockPersister::default(), test_worker_config(3, 1));

        let result = worker.process_realtime_item(1500).await;
        assert_eq!(result.outcome.kind, ItemOutcomeKind::TerminalMissing);
        assert_eq!(result.outcome.item_id, 1500);
    }
}
