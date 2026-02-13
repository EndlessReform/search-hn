use crate::firebase_listener::listener::Item as FirebaseItem;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::RateLimiter;
use std::sync::Arc;
use std::time::Duration;

/// Immutable segment assignment passed from orchestrator to worker.
///
/// The worker never claims or mutates segment ownership in storage; it only executes
/// one attempt over this assignment and reports what happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentWork {
    pub segment_id: i64,
    pub start_id: i64,
    pub end_id: i64,
    pub resume_cursor_id: Option<i64>,
}

impl SegmentWork {
    /// Validates coordinator-provided segment bounds.
    ///
    /// This intentionally fails fast if orchestrator input is invalid. Producing a clear
    /// fatal result is safer than attempting to "recover" from malformed ranges.
    pub fn validate(&self) -> Result<(), String> {
        if self.segment_id <= 0 {
            return Err(format!(
                "segment_id must be positive, got {}",
                self.segment_id
            ));
        }
        if self.start_id <= 0 {
            return Err(format!("start_id must be positive, got {}", self.start_id));
        }
        if self.start_id > self.end_id {
            return Err(format!(
                "start_id ({}) must be <= end_id ({})",
                self.start_id, self.end_id
            ));
        }

        if let Some(cursor) = self.resume_cursor_id {
            if cursor < self.start_id - 1 || cursor > self.end_id {
                return Err(format!(
                    "resume_cursor_id ({cursor}) must be in [{}..{}]",
                    self.start_id - 1,
                    self.end_id
                ));
            }
        }

        Ok(())
    }

    /// Returns the next ID the worker should process.
    pub fn next_id(&self) -> Result<i64, String> {
        self.validate()?;
        Ok(self
            .resume_cursor_id
            .map_or(self.start_id, |cursor| cursor + 1))
    }
}

/// Configures worker-level micro retry behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts including the first attempt.
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub jitter: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            jitter: Duration::from_millis(25),
        }
    }
}

/// Configures write batching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchPolicy {
    pub max_items: usize,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self { max_items: 500 }
    }
}

/// Worker settings used for both segment and realtime ingestion paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IngestWorkerConfig {
    pub retry_policy: RetryPolicy,
    pub batch_policy: BatchPolicy,
}

/// Shared process-local limiter used to enforce one global request budget across all
/// async ingest workers in a catchup run.
pub type GlobalRateLimiter = Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>;

/// Outcome from a low-level fetch call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchItemResponse {
    Found(FirebaseItem),
    Missing,
}

/// Normalized fetch failure classes used by worker retry logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchErrorKind {
    Network,
    RateLimited,
    UpstreamUnavailable,
    Unauthorized,
    Forbidden,
    MalformedResponse,
    Other,
}

/// Typed fetch failure with human-readable details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchError {
    pub kind: FetchErrorKind,
    pub message: String,
}

impl FetchError {
    pub fn new(kind: FetchErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.kind,
            FetchErrorKind::Network
                | FetchErrorKind::RateLimited
                | FetchErrorKind::UpstreamUnavailable
        )
    }
}

/// Normalized persistence failure classes used by worker retry logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistErrorKind {
    Retryable,
    Fatal,
}

/// Typed persistence failure with human-readable details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistError {
    pub kind: PersistErrorKind,
    pub message: String,
}

impl PersistError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            kind: PersistErrorKind::Retryable,
            message: message.into(),
        }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            kind: PersistErrorKind::Fatal,
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.kind == PersistErrorKind::Retryable
    }
}

/// Shared per-item outcome used by both segment and realtime ingestion paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemOutcomeKind {
    Succeeded,
    TerminalMissing,
    RetryableFailure,
    FatalFailure,
}

/// Rich per-item outcome with attempt count and failure details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemOutcome {
    pub item_id: i64,
    pub kind: ItemOutcomeKind,
    pub attempts: u32,
    pub message: Option<String>,
}

/// Top-level result of a single segment processing attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentAttemptStatus {
    Completed,
    RetryableFailure,
    FatalFailure,
}

/// Aggregate attempt result returned to orchestrator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentAttemptResult {
    pub segment_id: i64,
    pub status: SegmentAttemptStatus,
    pub last_durable_id: Option<i64>,
    pub unresolved_count: i32,
    pub terminal_missing_ids: Vec<i64>,
    pub failure: Option<ItemOutcome>,
}

/// Result of one realtime item ingestion attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealtimeIngestResult {
    pub outcome: ItemOutcome,
}
