use super::worker::worker;
use crate::catchup::error::Error;
use crate::queue::RangesQueue;

use diesel_async::pooled_connection::deadpool::Pool as DBPool;
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("Worker task joined with an error: {0}")]
    JoinError(String),
    #[error("Worker encountered an error: {0}")]
    WorkerError(#[from] Error), // Assuming Error is your existing error type
}

#[derive(Error, Debug)]
pub struct ShutdownError {
    errors: Vec<WorkerError>,
}

impl std::fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Errors during shutdown: {:?}", self.errors)
    }
}

pub struct WorkerPool {
    workers: Vec<JoinHandle<Result<(), Error>>>,
}

impl WorkerPool {
    pub fn new(
        num_workers: usize,
        firebase_url: &str,
        db_pool: DBPool<diesel_async::AsyncPgConnection>,
        queue: Arc<RangesQueue>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(2000u32))));
        let workers = (0..num_workers)
            .map(|_| {
                let worker_queue = queue.clone();
                let worker_db_pool = db_pool.clone();
                let worker_token = cancellation_token.clone();
                let fb_url = firebase_url.to_string();
                let worker_limiter = rate_limiter.clone();
                tokio::spawn(async move {
                    worker(
                        &fb_url,
                        worker_db_pool,
                        worker_queue,
                        worker_limiter,
                        worker_token,
                    )
                    .await
                })
            })
            .collect();
        Self { workers }
    }

    pub async fn wait_for_completion(self) -> Result<(), ShutdownError> {
        let mut errors = vec![];
        for handle in self.workers {
            match handle.await {
                Ok(Ok(())) => (), // Worker exited successfully
                Ok(Err(e)) => errors.push(WorkerError::WorkerError(e)),
                Err(e) => errors.push(WorkerError::JoinError(e.to_string())),
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ShutdownError { errors })
        }
    }
}
