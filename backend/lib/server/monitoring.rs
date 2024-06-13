use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use prometheus_client::registry::Registry;
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct CatchupMetrics {
    pub records_pulled: Counter,
    pub error_count: Counter,
}

impl CatchupMetrics {
    fn init() -> Self {
        Self {
            records_pulled: Counter::default(),
            error_count: Counter::default(),
        }
    }

    pub fn register(registry: &mut Registry, prefix: &str) -> Self {
        let metrics = Self::init();
        let sub_registry = registry.sub_registry_with_prefix(prefix);
        sub_registry.register(
            "records_pulled",
            "Total number of records pulled",
            metrics.records_pulled.clone(),
        );
        sub_registry.register(
            "error_count",
            "Total number of errors encountered",
            metrics.error_count.clone(),
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
