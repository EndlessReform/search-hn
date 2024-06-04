use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use prometheus_client::registry::Registry;

pub fn create_registry() -> Registry {
    let mut registry = Registry::default();

    // let crawl_status: Gauge = Gauge::default();
    // registry.register("crawl_status", "Status of the crawl process", crawl_status);

    let records_pulled_total: Counter = Counter::default();
    registry.register(
        "records_pulled_total",
        "Total number of records pulled",
        records_pulled_total,
    );

    let error_count_total: Counter = Counter::default();
    registry.register(
        "error_count_total",
        "Total number of errors encountered",
        error_count_total,
    );

    registry
}
