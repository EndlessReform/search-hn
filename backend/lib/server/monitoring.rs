use prometheus_client::metrics::counter::Counter;
use prometheus_client::registry::Registry;
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct CrawlerMetrics {
    pub records_pulled: Counter,
    pub error_count: Counter,
}

impl CrawlerMetrics {
    fn init() -> Self {
        Self {
            records_pulled: Counter::default(),
            error_count: Counter::default(),
        }
    }

    pub fn register(registry: &mut Registry) -> Self {
        let metrics = Self::init();

        registry.register(
            "records_pulled",
            "Total number of records pulled",
            metrics.records_pulled.clone(),
        );
        registry.register(
            "error_count",
            "Total number of errors encountered",
            metrics.error_count.clone(),
        );

        metrics
    }
}

pub static CRAWLER_METRICS: OnceCell<CrawlerMetrics> = OnceCell::const_new();
