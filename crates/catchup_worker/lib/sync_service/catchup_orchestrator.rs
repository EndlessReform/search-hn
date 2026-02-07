use diesel::pg::PgConnection;
use diesel::Connection;
use diesel_async::pooled_connection::deadpool::Pool;
use flume::{Receiver, Sender};
use log::{info, warn};
use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::error::Error;
use super::ingest_worker::{FirebaseItemFetcher, IngestWorker, PgBatchPersister};
use super::types::{IngestWorkerConfig, SegmentAttemptResult, SegmentAttemptStatus, SegmentWork};
use crate::firebase_listener::FirebaseListener;
use crate::segment_manager::{
    claim_next_pending_segment, compute_frontier_id, mark_segment_dead_letter, mark_segment_done,
    mark_segment_retry_wait, prepare_catchup_segments, update_segment_progress, upsert_exception,
    ExceptionState,
};

/// Configuration for catchup orchestration.
#[derive(Debug, Clone, Copy)]
pub struct CatchupOrchestratorConfig {
    /// Number of segment-processing workers.
    pub worker_count: usize,
    /// Segment width used by planning bootstrap when materializing missing work.
    pub segment_width: i64,
    /// Capacity of the in-memory work-stealing queue.
    pub queue_capacity: usize,
    /// Per-worker micro-retry and batching behavior.
    pub ingest_worker: IngestWorkerConfig,
}

impl Default for CatchupOrchestratorConfig {
    fn default() -> Self {
        Self {
            worker_count: 16,
            segment_width: 1000,
            queue_capacity: 1024,
            ingest_worker: IngestWorkerConfig::default(),
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

/// Coordinates catchup planning, segment claiming, worker fanout, and result persistence.
#[derive(Clone)]
pub struct CatchupOrchestrator {
    db_url: String,
    firebase_url: String,
    db_pool: Pool<diesel_async::AsyncPgConnection>,
    config: CatchupOrchestratorConfig,
}

impl CatchupOrchestrator {
    pub fn new(
        db_url: String,
        firebase_url: String,
        db_pool: Pool<diesel_async::AsyncPgConnection>,
        config: CatchupOrchestratorConfig,
    ) -> Self {
        Self {
            db_url,
            firebase_url,
            db_pool,
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
    ) -> Result<CatchupSyncSummary, Error> {
        validate_catchup_inputs(catchup_limit, cold_start_id)?;

        let fb = FirebaseListener::new(self.firebase_url.clone())?;
        let max_fb_id = fb.get_max_id().await?;

        let frontier_before = self
            .run_segment_op(|conn| compute_frontier_id(conn).map_err(Error::from))
            .await?;
        let target_max_id =
            resolve_target_max_id(max_fb_id, frontier_before, cold_start_id, catchup_limit);

        info!(
            "Catchup orchestrator planning target: frontier={} maxitem={} target={} cold_start={:?} limit={:?}",
            frontier_before, max_fb_id, target_max_id, cold_start_id, catchup_limit
        );

        let segment_width = self.config.segment_width;
        let preparation = self
            .run_segment_op(move |conn| {
                prepare_catchup_segments(conn, target_max_id, cold_start_id, segment_width)
                    .map_err(Error::from)
            })
            .await?;

        info!(
            "Catchup preparation complete: frontier={} planning_start={} target={} requeued_in_progress={} reactivated_retry_wait={} created_segments={} pending_window_segments={}",
            preparation.frontier_id,
            preparation.planning_start_id,
            preparation.target_max_id,
            preparation.requeued_in_progress,
            preparation.reactivated_retry_wait,
            preparation.created_segment_ids.len(),
            preparation.pending_segments.len(),
        );

        if preparation.pending_segments.is_empty() {
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
            worker_handles.push(tokio::spawn(async move {
                orchestrator
                    .run_worker_loop(worker_idx, worker_receiver, worker_fatal)
                    .await
            }));
        }

        let producer_orchestrator = self.clone();
        let producer_fatal = fatal_seen.clone();
        let producer_handle = tokio::spawn(async move {
            producer_orchestrator
                .enqueue_claimed_segments(sender, producer_fatal)
                .await
        });

        let claimed_segments = producer_handle.await??;

        let mut worker_rollup = WorkerLoopSummary::default();
        for handle in worker_handles {
            let worker_summary = handle.await??;
            worker_rollup.merge(worker_summary);
        }

        let had_fatal_failures = fatal_seen.load(Ordering::Relaxed);
        if had_fatal_failures {
            warn!(
                "Catchup run observed fatal failures; some claimed segments may remain in-progress until next resume"
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
    ) -> Result<WorkerLoopSummary, Error> {
        let fetcher = FirebaseItemFetcher::new(self.firebase_url.clone())?;
        let persister = PgBatchPersister::new(self.db_pool.clone());
        let ingest_worker = IngestWorker::new(fetcher, persister, self.config.ingest_worker);

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
                }
                SegmentAttemptStatus::RetryableFailure => {
                    summary.retry_wait_segments += 1;
                }
                SegmentAttemptStatus::FatalFailure => {
                    summary.dead_letter_segments += 1;
                    fatal_seen.store(true, Ordering::Relaxed);
                    warn!(
                        "Worker {} observed fatal segment failure on segment {}",
                        worker_idx, result.segment_id
                    );
                    break;
                }
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

        Ok(claimed)
    }

    async fn persist_attempt_result(&self, result: SegmentAttemptResult) -> Result<(), Error> {
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
                    let message = failure
                        .message
                        .clone()
                        .unwrap_or_else(|| "retryable segment failure".to_string());

                    upsert_exception(
                        conn,
                        result.segment_id,
                        failure.item_id,
                        ExceptionState::RetryWait,
                        attempts,
                        Some(message.clone()),
                    )?;
                    mark_segment_retry_wait(conn, result.segment_id, message)?;
                }
                SegmentAttemptStatus::FatalFailure => {
                    let failure = result.failure.as_ref().ok_or_else(|| {
                        Error::Orchestration(format!(
                            "segment {} fatal result missing failure payload",
                            result.segment_id
                        ))
                    })?;
                    let attempts = to_i32_attempts(failure.attempts)?;
                    let message = failure
                        .message
                        .clone()
                        .unwrap_or_else(|| "fatal segment failure".to_string());

                    upsert_exception(
                        conn,
                        result.segment_id,
                        failure.item_id,
                        ExceptionState::DeadLetter,
                        attempts,
                        Some(message.clone()),
                    )?;
                    mark_segment_dead_letter(conn, result.segment_id, message)?;
                }
            }

            Ok(())
        })
        .await
    }

    async fn run_segment_op<T, F>(&self, op: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> Result<T, Error> + Send + 'static,
    {
        let db_url = self.db_url.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = PgConnection::establish(&db_url).map_err(|err| {
                Error::ConnectError(format!("failed to connect to postgres: {err}"))
            })?;
            op(&mut conn)
        })
        .await?
    }
}

fn validate_catchup_inputs(
    catchup_limit: Option<i64>,
    cold_start_id: Option<i64>,
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

    Ok(())
}

fn resolve_target_max_id(
    max_fb_id: i64,
    frontier_id: i64,
    cold_start_id: Option<i64>,
    catchup_limit: Option<i64>,
) -> i64 {
    let planning_start = cold_start_id.unwrap_or_else(|| frontier_id.saturating_add(1));

    match catchup_limit {
        Some(limit) => {
            let span = limit.saturating_sub(1);
            let requested = planning_start.saturating_add(span);
            requested.min(max_fb_id)
        }
        None => max_fb_id,
    }
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
    use super::resolve_target_max_id;

    #[test]
    fn resolve_target_respects_limit_and_clamps_to_upstream_max() {
        let target = resolve_target_max_id(1_000, 500, Some(700), Some(200));
        assert_eq!(target, 899);

        let clamped = resolve_target_max_id(1_000, 500, Some(950), Some(200));
        assert_eq!(clamped, 1_000);
    }

    #[test]
    fn resolve_target_without_limit_uses_upstream_max() {
        let target = resolve_target_max_id(50_000, 20_000, None, None);
        assert_eq!(target, 50_000);
    }
}
