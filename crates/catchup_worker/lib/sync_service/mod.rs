mod catchup_orchestrator;
mod error;
mod firebase_worker;
pub mod ingest_worker;
pub mod types;

use diesel_async::pooled_connection::deadpool::Pool;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use std::sync::Arc;
use tracing::{info, warn, Instrument};

use catchup_orchestrator::CatchupOrchestrator;
pub use catchup_orchestrator::{CatchupOrchestratorConfig, CatchupSyncSummary};
use error::Error;
use firebase_worker::{worker, WorkerMode};

pub struct SyncService {
    /// Pool for async data-plane writes (`items`/`kids`).
    db_pool: Pool<diesel_async::AsyncPgConnection>,
    firebase_url: String,
    num_workers: usize,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl SyncService {
    pub fn new(
        firebase_url: String,
        db_pool: Pool<diesel_async::AsyncPgConnection>,
        num_workers: usize,
    ) -> Self {
        let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(2000u32))));
        Self {
            db_pool,
            num_workers,
            firebase_url,
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
                ..CatchupOrchestratorConfig::default()
            },
        )
        .await
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
        mut orchestrator_config: CatchupOrchestratorConfig,
    ) -> Result<(), Error> {
        if orchestrator_config.worker_count == 0 {
            orchestrator_config.worker_count = self.num_workers.max(1);
        }

        let orchestrator = CatchupOrchestrator::new(
            self.firebase_url.clone(),
            self.db_pool.clone(),
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

        Ok(())
    }

    /// Realtime subscription to HN item updates.
    ///
    /// Note: this remains the legacy path for now and will be migrated to the new orchestrator
    /// primitives in the next refactor step.
    pub async fn realtime_update(
        &self,
        num_workers: usize,
        receiver: flume::Receiver<i64>,
    ) -> Result<(), Error> {
        info!(
            event = "realtime_workers_spawning",
            worker_count = num_workers,
            "spawning realtime update workers"
        );
        let mut update_worker_handles = Vec::new();
        for _ in 0..num_workers {
            let worker_receiver = receiver.clone();
            let firebase_url = self.firebase_url.clone();
            let db_pool = self.db_pool.clone();
            let rate_limiter = Arc::clone(&self.rate_limiter);
            let handle = tokio::spawn(
                async move {
                    worker(
                        &firebase_url,
                        None,
                        None,
                        db_pool,
                        WorkerMode::Updater,
                        Some(worker_receiver),
                        rate_limiter,
                    )
                    .await
                }
                .in_current_span(),
            );
            update_worker_handles.push(handle);
        }
        info!(
            event = "realtime_workers_spawned",
            worker_count = num_workers,
            "successfully spawned realtime update workers"
        );
        Ok(())
    }
}
