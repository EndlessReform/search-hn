mod core;
mod error_mapping;
mod fetcher;
mod persister;
mod realtime_executor;
mod retry;
mod segment_executor;

pub use fetcher::{FirebaseItemFetcher, ItemFetcher};
pub use persister::{BatchPersister, PgBatchPersister};
pub use realtime_executor::RealtimeIngestExecutor;
pub use segment_executor::SegmentIngestExecutor;

#[cfg(test)]
mod realtime_tests;
#[cfg(test)]
mod segment_tests;
#[cfg(test)]
mod test_support;
