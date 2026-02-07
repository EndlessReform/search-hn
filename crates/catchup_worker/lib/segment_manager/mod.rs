//! Durable segment state management for catchup/healing.
//!
//! Why this module is separate from `db/`:
//! - The logic here is a control-plane policy surface (claim/requeue/frontier/dead-letter), not
//!   generic database plumbing.
//! - Keeping it in its own module makes orchestration invariants easier to reason about.
//!
//! Why this module is synchronous:
//! - Segment operations are tiny metadata updates compared to the high-throughput item ingest path.
//! - The data-plane (`items`/`kids`) remains async for throughput.
//! - A sync control plane keeps SQLite-backed unit tests fast and simple.
//!
//! Async callers should run these operations in `tokio::task::spawn_blocking` so Tokio runtime
//! worker threads are not blocked.

mod ops;
mod store;
mod types;

pub use ops::{
    claim_next_pending_segment, compute_frontier_id, enqueue_range, enqueue_segment, get_segment,
    list_exceptions_by_state, list_segments_by_status, mark_segment_dead_letter, mark_segment_done,
    mark_segment_retry_wait, prepare_catchup_segments, requeue_in_progress_segments,
    requeue_retry_wait_segments, set_segment_pending, update_segment_progress, upsert_exception,
};
pub use types::{
    CatchupPreparation, ExceptionState, IngestException, IngestSegment, SegmentStateError,
    SegmentStatus,
};
