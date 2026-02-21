mod catchup_orchestrator;
mod error;
mod firebase_worker;
pub mod ingest_worker;
pub mod types;
pub mod updater_state;

use diesel_async::pooled_connection::deadpool::Pool;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use catchup_orchestrator::CatchupOrchestrator;
pub use catchup_orchestrator::{CatchupOrchestratorConfig, CatchupSyncSummary};
use error::Error;
use ingest_worker::{FirebaseItemFetcher, IngestWorker, PgBatchPersister};
use types::{IngestWorkerConfig, ItemOutcomeKind};

use crate::server::monitoring::REALTIME_METRICS;

pub struct SyncService {
    /// Pool for async data-plane writes (`items`/`kids`).
    db_pool: Pool<diesel_async::AsyncPgConnection>,
    firebase_url: String,
    /// Stable identifier for the current worker run used in structured failure payloads.
    run_id: String,
    num_workers: usize,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl SyncService {
    pub fn new(
        firebase_url: String,
        db_pool: Pool<diesel_async::AsyncPgConnection>,
        run_id: String,
        num_workers: usize,
    ) -> Self {
        let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(2000u32))));
        Self {
            db_pool,
            num_workers,
            firebase_url,
            run_id,
            rate_limiter,
        }
    }

    /// Runs catchup via durable segment planning + worker orchestration.
    pub async fn catchup(
        &self,
        catchup_limit: Option<i64>,
        catchup_start: Option<i64>,
    ) -> Result<(), Error> {
        self.catchup_with_orchestrator_config(
            catchup_limit,
            catchup_start,
            None,
            false,
            CatchupOrchestratorConfig {
                worker_count: self.num_workers,
                queue_capacity: self.num_workers.max(1).saturating_mul(2),
                ..CatchupOrchestratorConfig::default()
            },
        )
        .await
    }

    /// Runs catchup with explicit orchestrator tuning knobs and returns the orchestration summary.
    pub async fn catchup_with_summary(
        &self,
        catchup_limit: Option<i64>,
        catchup_start: Option<i64>,
        catchup_end: Option<i64>,
        ignore_highest: bool,
        mut orchestrator_config: CatchupOrchestratorConfig,
    ) -> Result<CatchupSyncSummary, Error> {
        if orchestrator_config.worker_count == 0 {
            orchestrator_config.worker_count = self.num_workers.max(1);
        }

        let orchestrator = CatchupOrchestrator::new(
            self.firebase_url.clone(),
            self.db_pool.clone(),
            self.run_id.clone(),
            orchestrator_config,
        );

        let summary = orchestrator
            .sync(catchup_limit, catchup_start, catchup_end, ignore_highest)
            .await?;

        info!(
            event = "catchup_summary",
            frontier_id = summary.frontier_id,
            planning_start_id = summary.planning_start_id,
            target_max_id = summary.target_max_id,
            created_segments = summary.created_segments,
            claimed_segments = summary.claimed_segments,
            completed_segments = summary.completed_segments,
            retry_wait_segments = summary.retry_wait_segments,
            dead_letter_segments = summary.dead_letter_segments,
            requeued_in_progress = summary.requeued_in_progress,
            reactivated_retry_wait = summary.reactivated_retry_wait,
            had_fatal_failures = summary.had_fatal_failures,
            "catchup run summary"
        );

        if summary.had_fatal_failures {
            warn!(
                event = "catchup_fatal_failures_observed",
                "catchup ended with fatal segment failures; inspect dead-letter rows before replay"
            );
            return Err(Error::Orchestration(
                "catchup encountered fatal failures; inspect dead-letter rows".to_string(),
            ));
        }

        Ok(summary)
    }

    /// Runs catchup with explicit orchestrator tuning knobs.
    ///
    /// Intended for operational entrypoints (for example `catchup_only`) that need to expose
    /// practical controls without recompiling.
    pub async fn catchup_with_orchestrator_config(
        &self,
        catchup_limit: Option<i64>,
        catchup_start: Option<i64>,
        catchup_end: Option<i64>,
        ignore_highest: bool,
        orchestrator_config: CatchupOrchestratorConfig,
    ) -> Result<(), Error> {
        self.catchup_with_summary(
            catchup_limit,
            catchup_start,
            catchup_end,
            ignore_highest,
            orchestrator_config,
        )
        .await
        .map(|_| ())
    }

    /// Supervises realtime update workers.
    ///
    /// Each worker loops forever on the bounded update channel and uses `IngestWorker` for
    /// per-item retry/upsert logic. If a task exits unexpectedly, we restart it after a short
    /// delay so transient panics don't silently disable realtime ingestion.
    pub async fn realtime_update_supervised(
        &self,
        num_workers: usize,
        receiver: flume::Receiver<i64>,
        cancel_token: CancellationToken,
        worker_config: IngestWorkerConfig,
    ) -> Result<(), Error> {
        assert!(num_workers > 0, "num_workers must be > 0");

        let fetcher = Arc::new(FirebaseItemFetcher::new(
            self.firebase_url.clone(),
            Arc::clone(&self.rate_limiter),
        )?);
        let persister = Arc::new(PgBatchPersister::new(self.db_pool.clone()));

        let mut workers = JoinSet::new();
        for worker_idx in 0..num_workers {
            workers.spawn(spawn_realtime_worker(
                worker_idx,
                receiver.clone(),
                cancel_token.clone(),
                Arc::clone(&fetcher),
                Arc::clone(&persister),
                worker_config,
            ));
        }

        if let Some(metrics) = REALTIME_METRICS.get() {
            metrics.worker_alive_count.set(num_workers as i64);
        }

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    if let Some(metrics) = REALTIME_METRICS.get() {
                        metrics.worker_alive_count.set(0);
                    }
                    return Ok(());
                }
                maybe_joined = workers.join_next() => {
                    let Some(joined) = maybe_joined else {
                        return Err(Error::Orchestration("all realtime workers exited".to_string()));
                    };

                    match joined {
                        Ok(worker_idx) => {
                            warn!(
                                event = "realtime_worker_exited",
                                worker_idx,
                                restart_after_ms = 1000,
                                "realtime worker exited unexpectedly; restarting"
                            );
                            if !cancel_token.is_cancelled() {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                workers.spawn(spawn_realtime_worker(
                                    worker_idx,
                                    receiver.clone(),
                                    cancel_token.clone(),
                                    Arc::clone(&fetcher),
                                    Arc::clone(&persister),
                                    worker_config,
                                ));
                            }
                        }
                        Err(err) => {
                            error!(
                                event = "realtime_worker_join_failed",
                                error = %err,
                                "realtime worker panicked; restarting replacement"
                            );
                            if !cancel_token.is_cancelled() {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                workers.spawn(spawn_realtime_worker(
                                    0,
                                    receiver.clone(),
                                    cancel_token.clone(),
                                    Arc::clone(&fetcher),
                                    Arc::clone(&persister),
                                    worker_config,
                                ));
                            }
                        }
                    }

                    if let Some(metrics) = REALTIME_METRICS.get() {
                        metrics.worker_alive_count.set(workers.len() as i64);
                    }
                }
            }
        }
    }
}

async fn spawn_realtime_worker(
    worker_idx: usize,
    receiver: flume::Receiver<i64>,
    cancel_token: CancellationToken,
    fetcher: Arc<FirebaseItemFetcher>,
    persister: Arc<PgBatchPersister>,
    worker_config: IngestWorkerConfig,
) -> usize {
    let worker = IngestWorker::new(fetcher, persister, worker_config);

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                break;
            }
            received = receiver.recv_async() => {
                let Ok(item_id) = received else {
                    break;
                };

                let result = worker.process_realtime_item(item_id).await;
                if let Some(metrics) = REALTIME_METRICS.get() {
                    metrics.queue_depth.set(receiver.len() as i64);
                }
                match result.outcome.kind {
                    ItemOutcomeKind::Succeeded | ItemOutcomeKind::TerminalMissing => {
                        if let Some(metrics) = REALTIME_METRICS.get() {
                            metrics.records_pulled.inc();
                            if matches!(result.outcome.kind, ItemOutcomeKind::Succeeded) {
                                metrics.items_updated_total.inc();
                            }
                        }
                    }
                    ItemOutcomeKind::RetryableFailure | ItemOutcomeKind::FatalFailure => {
                        if let Some(metrics) = REALTIME_METRICS.get() {
                            metrics.records_failed.inc();
                        }
                        warn!(
                            event = "realtime_worker_item_failed",
                            worker_idx,
                            item_id,
                            outcome = ?result.outcome.kind,
                            failure_class = ?result.outcome.failure_class,
                            message = ?result.outcome.message,
                            "realtime item processing failed"
                        );
                    }
                }
            }
        }
    }

    worker_idx
}
