use deadpool_redis::{Config, Pool, Runtime};
use log::debug;
use rand::Rng;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Serialize, Deserialize, Debug)]
pub enum MessageType {
    Catchup,
    Realtime,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Job {
    pub start: i64,
    pub end: i64,
    pub message_type: MessageType,
}

impl Job {
    pub fn catchup(range: (i64, i64)) -> Self {
        Self {
            start: range.0,
            end: range.1,
            message_type: MessageType::Catchup,
        }
    }
    pub fn realtime(range: (i64, i64)) -> Self {
        Self {
            start: range.0,
            end: range.1,
            message_type: MessageType::Realtime,
        }
    }
}

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Pool error: {0}")]
    Pool(#[from] deadpool_redis::PoolError),

    #[error("Failed to parse job data: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Other error: {0}")]
    Other(String),
}

pub struct RangesQueue {
    pool: Pool,
    queue_key: String,
}

impl RangesQueue {
    pub async fn new(redis_url: &str, queue_key: &str) -> Result<Self, QueueError> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| QueueError::Other(e.to_string()))?;

        Ok(Self {
            pool,
            queue_key: queue_key.to_owned(),
        })
    }

    pub async fn push(&self, job: &Job) -> Result<(), QueueError> {
        let mut conn = self.pool.get().await?;
        let score = match job.message_type {
            MessageType::Realtime => f64::MAX,
            MessageType::Catchup => 0.0,
        };
        conn.zadd(&self.queue_key, serde_json::to_string(job)?, score)
            .await?;
        Ok(())
    }

    pub async fn pop(&self) -> Result<Option<Job>, QueueError> {
        // TODO Fix the connection starvation problem
        let mut conn = self.pool.get().await?;

        // Add 0-100ms jitter
        let jitter = rand::thread_rng().gen_range(0.0..0.1);
        let result: Option<(String, String, String)> =
            conn.bzpopmax(&self.queue_key, 5.0 + jitter).await?;
        match result {
            Some(s) => {
                debug!("{:?}", &s);
                Ok(Some(serde_json::from_str(&s.1)?))
            }
            None => {
                debug!("Useless result: {:?}", result);
                Ok(None)
            }
        }
    }
}
