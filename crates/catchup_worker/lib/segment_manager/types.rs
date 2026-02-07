use thiserror::Error;

/// Error type for segment-control CRUD operations.
#[derive(Debug, Error)]
pub enum SegmentStateError {
    #[error("database operation failed: {0}")]
    Database(#[from] diesel::result::Error),
    #[error("invalid segment status value in database: {0}")]
    InvalidSegmentStatus(String),
    #[error("invalid exception state value in database: {0}")]
    InvalidExceptionState(String),
    #[error("invalid segment-state input: {0}")]
    InvalidInput(String),
}

/// Durable lifecycle states for an ingest segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentStatus {
    Pending,
    InProgress,
    Done,
    RetryWait,
    DeadLetter,
}

impl SegmentStatus {
    pub(crate) fn as_db_str(self) -> &'static str {
        match self {
            SegmentStatus::Pending => "pending",
            SegmentStatus::InProgress => "in_progress",
            SegmentStatus::Done => "done",
            SegmentStatus::RetryWait => "retry_wait",
            SegmentStatus::DeadLetter => "dead_letter",
        }
    }

    pub(crate) fn from_db_str(value: &str) -> Result<Self, SegmentStateError> {
        match value {
            "pending" => Ok(SegmentStatus::Pending),
            "in_progress" => Ok(SegmentStatus::InProgress),
            "done" => Ok(SegmentStatus::Done),
            "retry_wait" => Ok(SegmentStatus::RetryWait),
            "dead_letter" => Ok(SegmentStatus::DeadLetter),
            other => Err(SegmentStateError::InvalidSegmentStatus(other.to_string())),
        }
    }
}

/// Durable lifecycle states for per-item exceptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExceptionState {
    Pending,
    RetryWait,
    TerminalMissing,
    DeadLetter,
}

impl ExceptionState {
    pub(crate) fn as_db_str(self) -> &'static str {
        match self {
            ExceptionState::Pending => "pending",
            ExceptionState::RetryWait => "retry_wait",
            ExceptionState::TerminalMissing => "terminal_missing",
            ExceptionState::DeadLetter => "dead_letter",
        }
    }

    pub(crate) fn from_db_str(value: &str) -> Result<Self, SegmentStateError> {
        match value {
            "pending" => Ok(ExceptionState::Pending),
            "retry_wait" => Ok(ExceptionState::RetryWait),
            "terminal_missing" => Ok(ExceptionState::TerminalMissing),
            "dead_letter" => Ok(ExceptionState::DeadLetter),
            other => Err(SegmentStateError::InvalidExceptionState(other.to_string())),
        }
    }
}

/// Materialized row from `ingest_segments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestSegment {
    pub segment_id: i64,
    pub start_id: i64,
    pub end_id: i64,
    pub status: SegmentStatus,
    pub attempts: i32,
    pub scan_cursor_id: Option<i64>,
    pub unresolved_count: i32,
    pub last_error: Option<String>,
}

/// Materialized row from `ingest_exceptions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestException {
    pub segment_id: i64,
    pub item_id: i64,
    pub state: ExceptionState,
    pub attempts: i32,
    pub last_error: Option<String>,
}

/// Summary of catchup preparation performed before worker fanout starts.
///
/// This struct captures the deterministic pre-flight control-plane work:
/// crash recovery (`in_progress` requeue), retry reactivation (`retry_wait` -> `pending`),
/// missing range segmentation, and the final pending queue slice for this target window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchupPreparation {
    pub frontier_id: i64,
    pub planning_start_id: i64,
    pub target_max_id: i64,
    pub requeued_in_progress: usize,
    pub reactivated_retry_wait: usize,
    pub created_segment_ids: Vec<i64>,
    pub pending_segments: Vec<IngestSegment>,
}
