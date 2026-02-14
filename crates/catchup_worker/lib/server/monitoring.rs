use crate::build_info;
use prometheus_client::metrics::info::Info;
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use prometheus_client::registry::Registry;
use tokio::sync::OnceCell;

/// Registers immutable build metadata for `/metrics` scraping.
///
/// This emits an OpenMetrics `info` metric that lets operators tie a running
/// process back to an exact artifact.
pub fn register_build_info_metric(registry: &mut Registry, prefix: &str) {
    let build_info_metric = Info::new(vec![
        ("service", "catchup_worker"),
        ("version", build_info::VERSION),
        ("commit", build_info::short_commit_hash()),
    ]);
    let sub_registry = registry.sub_registry_with_prefix(prefix);
    sub_registry.register(
        "build",
        "Build identity labels for this process",
        build_info_metric,
    );
}

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
    /// Total number of IDs in the selected catchup target window.
    pub target_total_items: Gauge,
    /// Number of IDs durably advanced in this run's target window.
    pub durable_items_completed: Gauge,
    /// Integer percent complete (0-100) for the active catchup target window.
    pub progress_percent: Gauge,
    /// Marker gauge: 1 when this run is a full-history catchup target, else 0.
    pub target_is_full_history: Gauge,
    /// Marker gauge: 1 when this run is a frontier/updater-style target, else 0.
    pub target_is_updater: Gauge,
    /// Marker gauge: 1 when this run is an explicitly bounded target, else 0.
    pub target_is_bounded: Gauge,
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
            target_total_items: Gauge::default(),
            durable_items_completed: Gauge::default(),
            progress_percent: Gauge::default(),
            target_is_full_history: Gauge::default(),
            target_is_updater: Gauge::default(),
            target_is_bounded: Gauge::default(),
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
        sub_registry.register(
            "target_total_items",
            "Total number of IDs in the selected catchup target window",
            metrics.target_total_items.clone(),
        );
        sub_registry.register(
            "durable_items_completed",
            "Number of IDs durably advanced in the active catchup target window",
            metrics.durable_items_completed.clone(),
        );
        sub_registry.register(
            "progress_percent",
            "Integer percent complete (0-100) for the active catchup target window",
            metrics.progress_percent.clone(),
        );
        sub_registry.register(
            "target_is_full_history",
            "Marker gauge: 1 for full-history catchup target, else 0",
            metrics.target_is_full_history.clone(),
        );
        sub_registry.register(
            "target_is_updater",
            "Marker gauge: 1 for frontier/updater-style catchup target, else 0",
            metrics.target_is_updater.clone(),
        );
        sub_registry.register(
            "target_is_bounded",
            "Marker gauge: 1 for explicitly bounded catchup target, else 0",
            metrics.target_is_bounded.clone(),
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

#[cfg(test)]
mod tests {
    use super::register_build_info_metric;
    use crate::build_info;
    use prometheus_client::{encoding::text::encode, registry::Registry};

    #[test]
    fn build_info_metric_contains_version_and_commit_labels() {
        let mut registry = Registry::default();
        register_build_info_metric(&mut registry, "worker");

        let mut encoded = String::new();
        encode(&mut encoded, &registry).expect("failed to encode metrics");

        assert!(
            encoded.contains("worker_build_info"),
            "expected a worker_build_info metric"
        );
        assert!(
            encoded.contains(&format!("version=\"{}\"", build_info::VERSION)),
            "expected build version label in metrics output"
        );
        assert!(
            encoded.contains(&format!("commit=\"{}\"", build_info::short_commit_hash())),
            "expected commit label in metrics output"
        );
    }
}
