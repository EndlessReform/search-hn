use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::pooled_connection::deadpool::Object as DeadpoolObject;
use diesel_async::pooled_connection::deadpool::Pool;
use flume::{Receiver, Sender};
use governor::{Quota, RateLimiter};
use serde_json::json;
use std::convert::TryFrom;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info, warn, Instrument};

use super::dlq::{insert_dlq_record, DlqRecord, DlqSource};
use super::error::Error;
use super::ingest_worker::{FirebaseItemFetcher, PgBatchPersister, SegmentIngestExecutor};
use super::types::{
    GlobalRateLimiter, IngestWorkerConfig, ItemOutcome, ItemOutcomeKind, SegmentAttemptResult,
    SegmentAttemptStatus, SegmentWork,
};
use crate::firebase_listener::FirebaseListener;
use crate::segment_manager::{
    claim_next_pending_segment, compute_frontier_id, mark_segment_dead_letter, mark_segment_done,
    mark_segment_retry_wait, prepare_catchup_segments, update_segment_progress, upsert_exception,
    ExceptionState,
};
use crate::server::monitoring::CATCHUP_METRICS;

/// Sync Diesel adapter over one pooled async Postgres connection.
///
/// Control-plane segment operations are intentionally synchronous today. We wrap a borrowed
/// pooled async connection and execute those operations in `spawn_blocking` to avoid reconnecting
/// on every control-plane query.
type ControlPlaneConn = AsyncConnectionWrapper<DeadpoolObject<diesel_async::AsyncPgConnection>>;

/// Configuration for catchup orchestration.
#[derive(Debug, Clone, Copy)]
pub struct CatchupOrchestratorConfig {
    /// Number of segment-processing workers.
    pub worker_count: usize,
    /// Segment width used by planning bootstrap when materializing missing work.
    pub segment_width: i64,
    /// Capacity of the in-memory work-stealing queue.
    pub queue_capacity: usize,
    /// Maximum HTTP requests per second across all catchup workers in this process.
    pub global_rps_limit: u32,
    /// Per-worker micro-retry and batching behavior.
    pub ingest_worker: IngestWorkerConfig,
    /// Debug flag: force the planning window back to `pending` and clear scan cursors so IDs are
    /// replayed even when already covered by done segments.
    pub force_replay_window: bool,
}

impl Default for CatchupOrchestratorConfig {
    fn default() -> Self {
        Self {
            worker_count: 16,
            segment_width: 1000,
            queue_capacity: 32,
            global_rps_limit: 250,
            ingest_worker: IngestWorkerConfig::default(),
            force_replay_window: false,
        }
    }
}

/// Outcome summary of one catchup `sync()` run.
#[derive(Debug, Clone)]
pub struct CatchupSyncSummary {
    pub target_max_id: i64,
    pub planning_start_id: i64,
    pub frontier_id: i64,
    pub requeued_in_progress: usize,
    pub reactivated_retry_wait: usize,
    pub created_segments: usize,
    pub claimed_segments: usize,
    pub completed_segments: usize,
    pub retry_wait_segments: usize,
    pub dead_letter_segments: usize,
    pub had_fatal_failures: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct WorkerLoopSummary {
    completed_segments: usize,
    retry_wait_segments: usize,
    dead_letter_segments: usize,
}

impl WorkerLoopSummary {
    fn merge(&mut self, other: WorkerLoopSummary) {
        self.completed_segments += other.completed_segments;
        self.retry_wait_segments += other.retry_wait_segments;
        self.dead_letter_segments += other.dead_letter_segments;
    }
}

/// High-level target profile for the current catchup run.
#[derive(Debug, Clone, Copy)]
enum CatchupTargetProfile {
    /// Full-range backfill (for example first boot or explicit `--start-id 1` without bounds).
    FullHistory,
    /// Frontier-driven incremental catchup (the normal updater-like path).
    Updater,
    /// Explicitly bounded replay/window catchup.
    Bounded,
}

impl CatchupTargetProfile {
    fn as_str(self) -> &'static str {
        match self {
            CatchupTargetProfile::FullHistory => "full_history",
            CatchupTargetProfile::Updater => "updater",
            CatchupTargetProfile::Bounded => "bounded",
        }
    }
}

/// Tracks run-local durable progress for percent-complete reporting.
#[derive(Debug)]
struct ProgressTracker {
    total_items: u64,
    completed_items: AtomicU64,
}

impl ProgressTracker {
    fn new(total_items: u64) -> Self {
        Self {
            total_items,
            completed_items: AtomicU64::new(0),
        }
    }

    fn record_advance(&self, delta: u64) -> u64 {
        if delta == 0 {
            return self.completed_items.load(Ordering::Relaxed);
        }

        let updated = self
            .completed_items
            .fetch_add(delta, Ordering::Relaxed)
            .saturating_add(delta);
        updated.min(self.total_items)
    }

    fn total_items(&self) -> u64 {
        self.total_items
    }

    fn percent_complete(&self) -> u64 {
        if self.total_items == 0 {
            return 100;
        }
        let completed = self
            .completed_items
            .load(Ordering::Relaxed)
            .min(self.total_items);
        completed.saturating_mul(100) / self.total_items
    }
}

/// Coordinates catchup planning, segment claiming, worker fanout, and result persistence.
#[derive(Clone)]
pub struct CatchupOrchestrator {
    firebase_url: String,
    db_pool: Pool<diesel_async::AsyncPgConnection>,
    run_id: String,
    config: CatchupOrchestratorConfig,
}

impl CatchupOrchestrator {
    pub fn new(
        firebase_url: String,
        db_pool: Pool<diesel_async::AsyncPgConnection>,
        run_id: String,
        config: CatchupOrchestratorConfig,
    ) -> Self {
        Self {
            firebase_url,
            db_pool,
            run_id,
            config,
        }
    }

    /// Runs one catchup synchronization pass.
    ///
    /// Steps:
    /// 1. Resolve current HN target and planning window.
    /// 2. Materialize/repair durable segment state (`prepare_catchup_segments`).
    /// 3. Claim pending segments into a work-stealing queue.
    /// 4. Execute segment workers and persist each attempt result back into segment state.
    pub async fn sync(
        &self,
        catchup_limit: Option<i64>,
        cold_start_id: Option<i64>,
        catchup_end_id: Option<i64>,
        ignore_highest: bool,
    ) -> Result<CatchupSyncSummary, Error> {
        validate_catchup_inputs(catchup_limit, cold_start_id, catchup_end_id, ignore_highest)?;
        let global_rate_limiter = build_global_rate_limiter(self.config.global_rps_limit)?;

        let fb = FirebaseListener::new(self.firebase_url.clone())?;
        let max_fb_id = fb.get_max_id().await?;
        info!(
            event = "catchup_worker_settings",
            worker_count = self.config.worker_count,
            segment_width = self.config.segment_width,
            queue_capacity = self.config.queue_capacity,
            global_rps_limit = self.config.global_rps_limit,
            batch_size = self.config.ingest_worker.batch_policy.max_items,
            retry_attempts = self.config.ingest_worker.retry_policy.max_attempts,
            retry_initial_ms = self
                .config
                .ingest_worker
                .retry_policy
                .initial_backoff
                .as_millis(),
            retry_max_ms = self
                .config
                .ingest_worker
                .retry_policy
                .max_backoff
                .as_millis(),
            retry_jitter_ms = self.config.ingest_worker.retry_policy.jitter.as_millis(),
            "resolved catchup worker settings"
        );

        let frontier_before = self
            .run_segment_op(|conn| compute_frontier_id(conn).map_err(Error::from))
            .await?;
        let target_max_id = resolve_target_max_id(
            max_fb_id,
            frontier_before,
            cold_start_id,
            catchup_limit,
            catchup_end_id,
            ignore_highest,
        )?;

        info!(
            event = "catchup_planning_target",
            frontier_id = frontier_before,
            maxitem = max_fb_id,
            target_max_id,
            cold_start_id = ?cold_start_id,
            catchup_limit = ?catchup_limit,
            catchup_end_id = ?catchup_end_id,
            ignore_highest,
            "resolved catchup planning target"
        );
        if let Some(metrics) = CATCHUP_METRICS.get() {
            metrics.frontier_id.set(frontier_before);
            metrics.target_max_id.set(target_max_id);
        }

        let segment_width = self.config.segment_width;
        let force_replay_window = self.config.force_replay_window;
        let preparation = self
            .run_segment_op(move |conn| {
                prepare_catchup_segments(
                    conn,
                    target_max_id,
                    cold_start_id,
                    segment_width,
                    force_replay_window,
                )
                .map_err(Error::from)
            })
            .await?;

        info!(
            event = "catchup_preparation_complete",
            frontier_id = preparation.frontier_id,
            planning_start_id = preparation.planning_start_id,
            target_max_id = preparation.target_max_id,
            requeued_in_progress = preparation.requeued_in_progress,
            reactivated_retry_wait = preparation.reactivated_retry_wait,
            created_segments = preparation.created_segment_ids.len(),
            pending_window_segments = preparation.pending_segments.len(),
            "prepared catchup segment state"
        );
        if let Some(metrics) = CATCHUP_METRICS.get() {
            metrics
                .pending_segments
                .set(i64::try_from(preparation.pending_segments.len()).unwrap_or(i64::MAX));
        }

        let target_profile = classify_target_profile(
            catchup_limit,
            cold_start_id,
            catchup_end_id,
            preparation.planning_start_id,
        );
        let progress_tracker = Arc::new(ProgressTracker::new(target_total_items(
            preparation.planning_start_id,
            preparation.target_max_id,
        )));
        info!(
            event = "catchup_target_profile",
            target_profile = target_profile.as_str(),
            target_total_items = progress_tracker.total_items(),
            "classified catchup target profile for metrics and dashboards"
        );
        if let Some(metrics) = CATCHUP_METRICS.get() {
            metrics
                .target_total_items
                .set(to_i64_gauge(progress_tracker.total_items()));
            metrics.durable_items_completed.set(0);
            metrics
                .progress_percent
                .set(to_i64_gauge(progress_tracker.percent_complete()));
            metrics.target_is_full_history.set(
                if matches!(target_profile, CatchupTargetProfile::FullHistory) {
                    1
                } else {
                    0
                },
            );
            metrics.target_is_updater.set(
                if matches!(target_profile, CatchupTargetProfile::Updater) {
                    1
                } else {
                    0
                },
            );
            metrics.target_is_bounded.set(
                if matches!(target_profile, CatchupTargetProfile::Bounded) {
                    1
                } else {
                    0
                },
            );
        }

        if preparation.pending_segments.is_empty() {
            if let Some(metrics) = CATCHUP_METRICS.get() {
                metrics
                    .durable_items_completed
                    .set(to_i64_gauge(progress_tracker.total_items()));
                metrics.progress_percent.set(100);
            }
            info!(
                event = "catchup_no_pending_segments",
                planning_start_id = preparation.planning_start_id,
                target_max_id = preparation.target_max_id,
                "no pending segments found for requested catchup window"
            );
            return Ok(CatchupSyncSummary {
                target_max_id: preparation.target_max_id,
                planning_start_id: preparation.planning_start_id,
                frontier_id: preparation.frontier_id,
                requeued_in_progress: preparation.requeued_in_progress,
                reactivated_retry_wait: preparation.reactivated_retry_wait,
                created_segments: preparation.created_segment_ids.len(),
                claimed_segments: 0,
                completed_segments: 0,
                retry_wait_segments: 0,
                dead_letter_segments: 0,
                had_fatal_failures: false,
            });
        }

        let (sender, receiver) = flume::bounded::<SegmentWork>(self.config.queue_capacity.max(1));
        let fatal_seen = Arc::new(AtomicBool::new(false));

        let mut worker_handles = Vec::new();
        for worker_idx in 0..self.config.worker_count.max(1) {
            let orchestrator = self.clone();
            let worker_receiver = receiver.clone();
            let worker_fatal = fatal_seen.clone();
            let worker_rate_limiter = global_rate_limiter.clone();
            let worker_progress_tracker = progress_tracker.clone();
            worker_handles.push(tokio::spawn(
                async move {
                    orchestrator
                        .run_worker_loop(
                            worker_idx,
                            worker_receiver,
                            worker_fatal,
                            worker_rate_limiter,
                            worker_progress_tracker,
                        )
                        .await
                }
                .in_current_span(),
            ));
        }

        let producer_orchestrator = self.clone();
        let producer_fatal = fatal_seen.clone();
        let producer_handle = tokio::spawn(
            async move {
                producer_orchestrator
                    .enqueue_claimed_segments(sender, producer_fatal)
                    .await
            }
            .in_current_span(),
        );

        let claimed_segments = producer_handle.await??;

        let mut worker_rollup = WorkerLoopSummary::default();
        for handle in worker_handles {
            let worker_summary = handle.await??;
            worker_rollup.merge(worker_summary);
        }

        let had_fatal_failures = fatal_seen.load(Ordering::Relaxed);
        if had_fatal_failures {
            warn!(
                event = "catchup_fatal_failure_seen",
                claimed_segments,
                "catchup run observed fatal failures; some claimed segments may remain in-progress until next resume"
            );
        }

        Ok(CatchupSyncSummary {
            target_max_id: preparation.target_max_id,
            planning_start_id: preparation.planning_start_id,
            frontier_id: preparation.frontier_id,
            requeued_in_progress: preparation.requeued_in_progress,
            reactivated_retry_wait: preparation.reactivated_retry_wait,
            created_segments: preparation.created_segment_ids.len(),
            claimed_segments,
            completed_segments: worker_rollup.completed_segments,
            retry_wait_segments: worker_rollup.retry_wait_segments,
            dead_letter_segments: worker_rollup.dead_letter_segments,
            had_fatal_failures,
        })
    }

    async fn run_worker_loop(
        &self,
        worker_idx: usize,
        receiver: Receiver<SegmentWork>,
        fatal_seen: Arc<AtomicBool>,
        rate_limiter: GlobalRateLimiter,
        progress_tracker: Arc<ProgressTracker>,
    ) -> Result<WorkerLoopSummary, Error> {
        let fetcher = FirebaseItemFetcher::new(self.firebase_url.clone(), rate_limiter)?;
        let persister = PgBatchPersister::new(self.db_pool.clone());
        let ingest_worker =
            SegmentIngestExecutor::new(fetcher, persister, self.config.ingest_worker);

        let mut summary = WorkerLoopSummary::default();

        while let Ok(work) = receiver.recv_async().await {
            if fatal_seen.load(Ordering::Relaxed) {
                break;
            }

            let result = ingest_worker.process_segment_once(&work).await;
            self.persist_attempt_result(result.clone()).await?;

            match result.status {
                SegmentAttemptStatus::Completed => {
                    summary.completed_segments += 1;
                    if let Some(metrics) = CATCHUP_METRICS.get() {
                        metrics.segments_completed_total.inc();
                    }
                }
                SegmentAttemptStatus::RetryableFailure => {
                    summary.retry_wait_segments += 1;
                    if let Some(metrics) = CATCHUP_METRICS.get() {
                        metrics.segments_retry_wait_total.inc();
                    }
                    if let Some(failure) = &result.failure {
                        let payload = build_failure_payload(
                            &self.run_id,
                            result.segment_id,
                            failure,
                            "retryable failure",
                        );
                        let diagnostics = failure.diagnostics.as_ref();
                        warn!(
                            event = "segment_retry_wait",
                            worker_idx,
                            segment_id = result.segment_id,
                            item_id = failure.item_id,
                            attempts = failure.attempts,
                            run_id = %self.run_id,
                            failure_class = %failure_class_for_log(failure),
                            reqwest_status = ?diagnostics.and_then(|diag| diag.status),
                            reqwest_url = ?diagnostics.and_then(|diag| diag.url.as_deref()),
                            reqwest_is_timeout = diagnostics.map_or(false, |diag| diag.is_timeout),
                            reqwest_is_connect = diagnostics.map_or(false, |diag| diag.is_connect),
                            reqwest_is_decode = diagnostics.map_or(false, |diag| diag.is_decode),
                            reqwest_error_chain = %diagnostics
                                .map(|diag| diag.error_chain.as_str())
                                .unwrap_or("n/a"),
                            reqwest_error_debug = %diagnostics
                                .map(|diag| diag.error_debug.as_str())
                                .unwrap_or("n/a"),
                            error = %failure
                                .message
                                .as_deref()
                                .unwrap_or("retryable failure"),
                            failure_payload = %payload,
                            "segment moved to retry_wait"
                        );
                    }
                }
                SegmentAttemptStatus::FatalFailure => {
                    summary.dead_letter_segments += 1;
                    if let Some(metrics) = CATCHUP_METRICS.get() {
                        metrics.segments_dead_letter_total.inc();
                    }
                    fatal_seen.store(true, Ordering::Relaxed);
                    if let Some(failure) = &result.failure {
                        let payload = build_failure_payload(
                            &self.run_id,
                            result.segment_id,
                            failure,
                            "fatal failure",
                        );
                        let diagnostics = failure.diagnostics.as_ref();
                        warn!(
                            event = "segment_dead_letter",
                            worker_idx,
                            segment_id = result.segment_id,
                            item_id = failure.item_id,
                            attempts = failure.attempts,
                            run_id = %self.run_id,
                            failure_class = %failure_class_for_log(failure),
                            reqwest_status = ?diagnostics.and_then(|diag| diag.status),
                            reqwest_url = ?diagnostics.and_then(|diag| diag.url.as_deref()),
                            reqwest_is_timeout = diagnostics.map_or(false, |diag| diag.is_timeout),
                            reqwest_is_connect = diagnostics.map_or(false, |diag| diag.is_connect),
                            reqwest_is_decode = diagnostics.map_or(false, |diag| diag.is_decode),
                            reqwest_error_chain = %diagnostics
                                .map(|diag| diag.error_chain.as_str())
                                .unwrap_or("n/a"),
                            reqwest_error_debug = %diagnostics
                                .map(|diag| diag.error_debug.as_str())
                                .unwrap_or("n/a"),
                            error = %failure
                                .message
                                .as_deref()
                                .unwrap_or("fatal failure"),
                            failure_payload = %payload,
                            "segment moved to dead_letter after fatal failure"
                        );
                    } else {
                        warn!(
                            event = "segment_dead_letter",
                            worker_idx,
                            segment_id = result.segment_id,
                            run_id = %self.run_id,
                            "segment moved to dead_letter after fatal failure with missing payload"
                        );
                    }
                    break;
                }
            }

            if let Some(metrics) = CATCHUP_METRICS.get() {
                let missing_count =
                    u64::try_from(result.terminal_missing_ids.len()).unwrap_or(u64::MAX);
                let durable_advance = durable_items_advanced(&work, &result);
                metrics.terminal_missing_items_total.inc_by(missing_count);
                metrics.durable_items_total.inc_by(durable_advance);
                let completed = progress_tracker.record_advance(durable_advance);
                metrics.durable_items_completed.set(to_i64_gauge(completed));
                metrics
                    .progress_percent
                    .set(to_i64_gauge(progress_tracker.percent_complete()));
            }
        }

        Ok(summary)
    }

    async fn enqueue_claimed_segments(
        &self,
        sender: Sender<SegmentWork>,
        fatal_seen: Arc<AtomicBool>,
    ) -> Result<usize, Error> {
        let mut claimed = 0usize;

        loop {
            if fatal_seen.load(Ordering::Relaxed) {
                break;
            }

            let maybe_segment = self
                .run_segment_op(|conn| claim_next_pending_segment(conn).map_err(Error::from))
                .await?;

            let Some(segment) = maybe_segment else {
                break;
            };

            claimed += 1;
            if let Some(metrics) = CATCHUP_METRICS.get() {
                metrics.segments_claimed_total.inc();
            }
            let work = SegmentWork {
                segment_id: segment.segment_id,
                start_id: segment.start_id,
                end_id: segment.end_id,
                resume_cursor_id: segment.scan_cursor_id,
            };

            if sender.send_async(work).await.is_err() {
                return Err(Error::Orchestration(
                    "segment queue closed before all claims were dispatched".to_string(),
                ));
            }
        }

        info!(
            event = "catchup_segments_claimed",
            claimed_segments = claimed,
            "claimed pending segments for catchup pass"
        );
        Ok(claimed)
    }

    async fn persist_attempt_result(&self, result: SegmentAttemptResult) -> Result<(), Error> {
        let run_id = self.run_id.clone();
        let dlq_records = build_dlq_records_for_segment(&run_id, &result);
        self.run_segment_op(move |conn| {
            if matches!(
                result.status,
                SegmentAttemptStatus::RetryableFailure | SegmentAttemptStatus::FatalFailure
            ) {
                update_segment_progress(
                    conn,
                    result.segment_id,
                    result.last_durable_id,
                    result.unresolved_count,
                )?;
            }

            for missing_id in &result.terminal_missing_ids {
                upsert_exception(
                    conn,
                    result.segment_id,
                    *missing_id,
                    ExceptionState::TerminalMissing,
                    1,
                    None,
                    None,
                )?;
            }

            match result.status {
                SegmentAttemptStatus::Completed => {
                    mark_segment_done(conn, result.segment_id, result.last_durable_id)?;
                }
                SegmentAttemptStatus::RetryableFailure => {
                    let failure = result.failure.as_ref().ok_or_else(|| {
                        Error::Orchestration(format!(
                            "segment {} retryable result missing failure payload",
                            result.segment_id
                        ))
                    })?;
                    let attempts = to_i32_attempts(failure.attempts)?;
                    let payload = build_failure_payload(
                        &run_id,
                        result.segment_id,
                        failure,
                        "retryable segment failure",
                    );
                    let failure_class = failure.failure_class.clone();

                    upsert_exception(
                        conn,
                        result.segment_id,
                        failure.item_id,
                        ExceptionState::RetryWait,
                        attempts,
                        Some(payload.clone()),
                        failure_class.clone(),
                    )?;
                    mark_segment_retry_wait(conn, result.segment_id, payload, failure_class)?;
                }
                SegmentAttemptStatus::FatalFailure => {
                    let failure = result.failure.as_ref().ok_or_else(|| {
                        Error::Orchestration(format!(
                            "segment {} fatal result missing failure payload",
                            result.segment_id
                        ))
                    })?;
                    let attempts = to_i32_attempts(failure.attempts)?;
                    let payload = build_failure_payload(
                        &run_id,
                        result.segment_id,
                        failure,
                        "fatal segment failure",
                    );
                    let failure_class = failure.failure_class.clone();

                    upsert_exception(
                        conn,
                        result.segment_id,
                        failure.item_id,
                        ExceptionState::DeadLetter,
                        attempts,
                        Some(payload.clone()),
                        failure_class.clone(),
                    )?;
                    mark_segment_dead_letter(conn, result.segment_id, payload, failure_class)?;
                }
            }

            Ok(())
        })
        .await?;

        for record in dlq_records {
            if let Err(err) = insert_dlq_record(&self.db_pool, &record).await {
                warn!(
                    event = "catchup_dlq_persist_failed",
                    segment_id = ?record.segment_id,
                    item_id = record.item_id,
                    state = ?record.state,
                    error = %err,
                    "failed to persist catchup DLQ record"
                );
            }
        }

        Ok(())
    }

    async fn run_segment_op<T, F>(&self, op: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&mut ControlPlaneConn) -> Result<T, Error> + Send + 'static,
    {
        let pooled_conn = self.db_pool.get().await?;
        tokio::task::spawn_blocking(move || {
            let mut conn = ControlPlaneConn::from(pooled_conn);
            op(&mut conn)
        })
        .await?
    }
}

fn failure_class_for_log(failure: &ItemOutcome) -> &str {
    failure.failure_class.as_deref().unwrap_or("unknown")
}

fn build_dlq_records_for_segment(run_id: &str, result: &SegmentAttemptResult) -> Vec<DlqRecord> {
    let mut records: Vec<DlqRecord> = result
        .terminal_missing_ids
        .iter()
        .map(|item_id| {
            DlqRecord::terminal_missing(
                DlqSource::Catchup,
                run_id.to_string(),
                Some(result.segment_id),
                *item_id,
            )
        })
        .collect();

    if let Some(failure) = result.failure.as_ref() {
        if let Some(mut record) = DlqRecord::from_item_outcome(
            DlqSource::Catchup,
            run_id.to_string(),
            Some(result.segment_id),
            failure,
        ) {
            let default_message = match failure.kind {
                ItemOutcomeKind::RetryableFailure => "retryable segment failure",
                ItemOutcomeKind::FatalFailure => "fatal segment failure",
                ItemOutcomeKind::TerminalMissing => "terminal missing item",
                ItemOutcomeKind::Succeeded => "unexpected success",
            };
            record.last_error = Some(build_failure_payload(
                run_id,
                result.segment_id,
                failure,
                default_message,
            ));
            records.push(record);
        }
    }

    records
}

/// Builds one consistent structured payload for retry/dead-letter persistence and logging.
///
/// Keeping this format stable means operators can inspect both journal entries and
/// `ingest_exceptions.last_error` using the same schema.
fn build_failure_payload(
    run_id: &str,
    segment_id: i64,
    failure: &ItemOutcome,
    default_message: &str,
) -> String {
    let message = failure
        .message
        .as_deref()
        .unwrap_or(default_message)
        .to_string();
    let diagnostics = failure.diagnostics.as_ref().map(|diag| {
        json!({
            "status": diag.status,
            "url": diag.url,
            "is_timeout": diag.is_timeout,
            "is_connect": diag.is_connect,
            "is_decode": diag.is_decode,
            "is_body": diag.is_body,
            "is_request": diag.is_request,
            "error_chain": diag.error_chain,
            "error_debug": diag.error_debug,
        })
    });

    json!({
        "run_id": run_id,
        "segment_id": segment_id,
        "item_id": failure.item_id,
        "attempts": failure.attempts,
        "failure_class": failure.failure_class.as_deref().unwrap_or("unknown"),
        "message": message,
        "diagnostics": diagnostics,
    })
    .to_string()
}

fn build_global_rate_limiter(global_rps_limit: u32) -> Result<GlobalRateLimiter, Error> {
    let per_second = NonZeroU32::new(global_rps_limit).ok_or_else(|| {
        Error::Orchestration(format!(
            "global_rps_limit must be > 0, got {global_rps_limit}"
        ))
    })?;
    Ok(Arc::new(RateLimiter::direct(Quota::per_second(per_second))))
}

fn validate_catchup_inputs(
    catchup_limit: Option<i64>,
    cold_start_id: Option<i64>,
    catchup_end_id: Option<i64>,
    ignore_highest: bool,
) -> Result<(), Error> {
    if let Some(limit) = catchup_limit {
        if limit <= 0 {
            return Err(Error::Orchestration(format!(
                "catchup limit must be > 0, got {limit}"
            )));
        }
    }

    if let Some(start_id) = cold_start_id {
        if start_id <= 0 {
            return Err(Error::Orchestration(format!(
                "catchup start must be > 0, got {start_id}"
            )));
        }
    }

    if let Some(end_id) = catchup_end_id {
        if end_id <= 0 {
            return Err(Error::Orchestration(format!(
                "catchup end must be > 0, got {end_id}"
            )));
        }
        if let Some(start_id) = cold_start_id {
            if end_id < start_id {
                return Err(Error::Orchestration(format!(
                    "catchup end ({end_id}) must be >= catchup start ({start_id})"
                )));
            }
        }
    }

    if catchup_limit.is_some() && catchup_end_id.is_some() {
        return Err(Error::Orchestration(
            "cannot set both catchup limit and catchup end".to_string(),
        ));
    }

    if ignore_highest && catchup_limit.is_none() && catchup_end_id.is_none() {
        return Err(Error::Orchestration(
            "ignore-highest requires either catchup limit or catchup end".to_string(),
        ));
    }

    Ok(())
}

fn resolve_target_max_id(
    max_fb_id: i64,
    frontier_id: i64,
    cold_start_id: Option<i64>,
    catchup_limit: Option<i64>,
    catchup_end_id: Option<i64>,
    ignore_highest: bool,
) -> Result<i64, Error> {
    let planning_start = cold_start_id.unwrap_or_else(|| frontier_id.saturating_add(1));

    if let Some(explicit_end_id) = catchup_end_id {
        return Ok(if ignore_highest {
            explicit_end_id
        } else {
            explicit_end_id.min(max_fb_id)
        });
    }

    match catchup_limit {
        Some(limit) => {
            let span = limit.saturating_sub(1);
            let requested = planning_start.saturating_add(span);
            Ok(if ignore_highest {
                requested
            } else {
                requested.min(max_fb_id)
            })
        }
        None => Ok(max_fb_id),
    }
}

fn classify_target_profile(
    catchup_limit: Option<i64>,
    cold_start_id: Option<i64>,
    catchup_end_id: Option<i64>,
    planning_start_id: i64,
) -> CatchupTargetProfile {
    if catchup_limit.is_none() && catchup_end_id.is_none() {
        if planning_start_id <= 1 {
            return CatchupTargetProfile::FullHistory;
        }
        if cold_start_id.is_none() {
            return CatchupTargetProfile::Updater;
        }
    }
    CatchupTargetProfile::Bounded
}

fn target_total_items(planning_start_id: i64, target_max_id: i64) -> u64 {
    if planning_start_id > target_max_id {
        return 0;
    }
    u64::try_from(target_max_id - planning_start_id + 1).unwrap_or(u64::MAX)
}

fn to_i64_gauge(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn durable_items_advanced(work: &SegmentWork, result: &SegmentAttemptResult) -> u64 {
    let next_id = match work.next_id() {
        Ok(value) => value,
        Err(_) => return 0,
    };

    let Some(last_durable) = result.last_durable_id else {
        return 0;
    };
    if last_durable < next_id {
        return 0;
    }

    u64::try_from(last_durable - next_id + 1).unwrap_or(u64::MAX)
}

fn to_i32_attempts(attempts: u32) -> Result<i32, Error> {
    i32::try_from(attempts).map_err(|_| {
        Error::Orchestration(format!(
            "attempt counter overflow: {attempts} cannot fit into i32"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        classify_target_profile, resolve_target_max_id, target_total_items, CatchupTargetProfile,
    };

    #[test]
    fn resolve_target_respects_limit_and_clamps_to_upstream_max() {
        let target = resolve_target_max_id(1_000, 500, Some(700), Some(200), None, false)
            .expect("target resolution should succeed");
        assert_eq!(target, 899);

        let clamped = resolve_target_max_id(1_000, 500, Some(950), Some(200), None, false)
            .expect("target resolution should succeed");
        assert_eq!(clamped, 1_000);
    }

    #[test]
    fn resolve_target_without_limit_uses_upstream_max() {
        let target = resolve_target_max_id(50_000, 20_000, None, None, None, false)
            .expect("target resolution should succeed");
        assert_eq!(target, 50_000);
    }

    #[test]
    fn resolve_target_honors_explicit_end_for_debug_replay() {
        let target = resolve_target_max_id(50_000, 20_000, Some(1), None, Some(100_000), true)
            .expect("target resolution should succeed");
        assert_eq!(target, 100_000);
    }

    #[test]
    fn classify_target_profile_distinguishes_full_history_and_updater() {
        let full_history = classify_target_profile(None, Some(1), None, 1);
        assert!(matches!(full_history, CatchupTargetProfile::FullHistory));

        let updater = classify_target_profile(None, None, None, 42);
        assert!(matches!(updater, CatchupTargetProfile::Updater));

        let bounded = classify_target_profile(Some(100), None, None, 42);
        assert!(matches!(bounded, CatchupTargetProfile::Bounded));
    }

    #[test]
    fn target_total_items_handles_empty_and_inclusive_windows() {
        assert_eq!(target_total_items(10, 9), 0);
        assert_eq!(target_total_items(10, 10), 1);
        assert_eq!(target_total_items(10, 12), 3);
    }
}
