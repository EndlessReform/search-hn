use deadpool_redis::{Config, Pool, Runtime};
use redis::AsyncCommands;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Pool error: {0}")]
    Pool(#[from] deadpool_redis::PoolError),

    #[error("Failed to parse job data: {0}")]
    Parse(String),

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

    fn parse_job(s: &str) -> Result<(i64, i64), QueueError> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() == 2 {
            let start = parts[0]
                .parse()
                .map_err(|_| QueueError::Parse(s.to_string()))?;
            let end = parts[1]
                .parse()
                .map_err(|_| QueueError::Parse(s.to_string()))?;
            Ok((start, end))
        } else {
            Err(QueueError::Parse(s.to_string()))
        }
    }
    pub async fn push(&self, job: (i64, i64)) -> Result<(), QueueError> {
        let mut conn = self.pool.get().await?;
        conn.lpush(&self.queue_key, format!("{},{}", job.0, job.1))
            .await?;
        Ok(())
    }

    pub async fn pop(&self) -> Result<Option<(i64, i64)>, QueueError> {
        let mut conn = self.pool.get().await?;
        let result: Option<String> = conn.rpop(&self.queue_key, None).await?;
        Ok(result.map(|s| Self::parse_job(&s)).transpose()?)
    }
}
