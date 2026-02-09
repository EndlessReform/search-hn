use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use prometheus_client::registry::Registry;
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct CatchupMetrics {
    /// Number of segments claimed from durable pending state.
    pub segments_claimed_total: Counter,
    /// Number of segments completed in this process lifetime.
    pub segments_completed_total: Counter,
    /// Number of segments transitioned to retry-wait due to retryable failure.
    pub segments_retry_wait_total: Counter,
    /// Number of segments transitioned to dead-letter due to fatal failure.
    pub segments_dead_letter_total: Counter,
    /// Number of terminal-missing item IDs observed during catchup.
    pub terminal_missing_items_total: Counter,
    /// Number of durable item IDs advanced by workers.
    ///
    /// This is a throughput-oriented counter. Use PromQL `rate()` for IDs/sec.
    pub durable_items_total: Counter,
    /// Latest frontier seen by orchestrator planning.
    pub frontier_id: Gauge,
    /// Latest target max ID chosen by orchestrator planning.
    pub target_max_id: Gauge,
    /// Pending segment count in the active planning window.
    pub pending_segments: Gauge,
}

impl CatchupMetrics {
    fn init() -> Self {
        Self {
            segments_claimed_total: Counter::default(),
            segments_completed_total: Counter::default(),
            segments_retry_wait_total: Counter::default(),
            segments_dead_letter_total: Counter::default(),
            terminal_missing_items_total: Counter::default(),
            durable_items_total: Counter::default(),
            frontier_id: Gauge::default(),
            target_max_id: Gauge::default(),
            pending_segments: Gauge::default(),
        }
    }

    pub fn register(registry: &mut Registry, prefix: &str) -> Self {
        let metrics = Self::init();
        let sub_registry = registry.sub_registry_with_prefix(prefix);
        sub_registry.register(
            "segments_claimed_total",
            "Total number of catchup segments claimed",
            metrics.segments_claimed_total.clone(),
        );
        sub_registry.register(
            "segments_completed_total",
            "Total number of catchup segments completed",
            metrics.segments_completed_total.clone(),
        );
        sub_registry.register(
            "segments_retry_wait_total",
            "Total number of catchup segments moved to retry_wait",
            metrics.segments_retry_wait_total.clone(),
        );
        sub_registry.register(
            "segments_dead_letter_total",
            "Total number of catchup segments moved to dead_letter",
            metrics.segments_dead_letter_total.clone(),
        );
        sub_registry.register(
            "terminal_missing_items_total",
            "Total number of terminal-missing item IDs observed in catchup",
            metrics.terminal_missing_items_total.clone(),
        );
        sub_registry.register(
            "durable_items_total",
            "Total number of durable item IDs advanced by catchup workers",
            metrics.durable_items_total.clone(),
        );
        sub_registry.register(
            "frontier_id",
            "Latest frontier ID seen during catchup planning",
            metrics.frontier_id.clone(),
        );
        sub_registry.register(
            "target_max_id",
            "Latest target max ID selected during catchup planning",
            metrics.target_max_id.clone(),
        );
        sub_registry.register(
            "pending_segments",
            "Pending segment count in the active catchup planning window",
            metrics.pending_segments.clone(),
        );
        metrics
    }
}

pub static CATCHUP_METRICS: OnceCell<CatchupMetrics> = OnceCell::const_new();

#[derive(Clone)]
pub struct RealtimeMetrics {
    pub batch_size: Gauge,
    pub records_pulled: Counter,
    pub records_failed: Counter,
}

impl RealtimeMetrics {
    fn init() -> Self {
        Self {
            batch_size: Gauge::default(),
            records_pulled: Counter::default(),
            records_failed: Counter::default(),
        }
    }

    pub fn register(registry: &mut Registry, prefix: &str) -> Self {
        let metrics = Self::init();
        let sub_registry = registry.sub_registry_with_prefix(prefix);
        sub_registry.register(
            "batch_size",
            "Number of records in each batch",
            metrics.batch_size.clone(),
        );
        sub_registry.register(
            "records_processed_total",
            "Total number of successfully processed records",
            metrics.records_pulled.clone(),
        );
        sub_registry.register(
            "records_failed_total",
            "Total number of records that failed processing",
            metrics.records_failed.clone(),
        );
        metrics
    }
}

pub static REALTIME_METRICS: OnceCell<RealtimeMetrics> = OnceCell::const_new();
