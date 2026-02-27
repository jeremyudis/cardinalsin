//! Query telemetry instruments and recording helpers.

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;
use std::sync::OnceLock;

struct QueryInstruments {
    requests_total: Counter<u64>,
    latency_seconds: Histogram<f64>,
    bytes_scanned_total: Counter<u64>,
    cache_hits_total: Counter<u64>,
    cache_misses_total: Counter<u64>,
}

fn instruments() -> &'static QueryInstruments {
    static INSTRUMENTS: OnceLock<QueryInstruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("cardinalsin.query");
        QueryInstruments {
            requests_total: meter
                .u64_counter("cardinalsin_query_requests_total")
                .with_description("Total query requests by result")
                .with_unit("1")
                .init(),
            latency_seconds: meter
                .f64_histogram("cardinalsin_query_latency_seconds")
                .with_description("End-to-end query latency")
                .with_unit("s")
                .init(),
            bytes_scanned_total: meter
                .u64_counter("cardinalsin_query_bytes_scanned_total")
                .with_description("Estimated bytes scanned by queries")
                .with_unit("By")
                .init(),
            cache_hits_total: meter
                .u64_counter("cardinalsin_query_cache_hits_total")
                .with_description("Total query cache hits")
                .with_unit("1")
                .init(),
            cache_misses_total: meter
                .u64_counter("cardinalsin_query_cache_misses_total")
                .with_description("Total query cache misses")
                .with_unit("1")
                .init(),
        }
    })
}

fn common_attrs() -> [KeyValue; 3] {
    [
        KeyValue::new("service", crate::telemetry::service()),
        KeyValue::new("run_id", crate::telemetry::run_id()),
        KeyValue::new("tenant", crate::telemetry::tenant()),
    ]
}

pub fn record_query_request(duration_seconds: f64, result: &'static str) {
    let i = instruments();
    let [service, run_id, tenant] = common_attrs();
    let attrs = [service, run_id, tenant, KeyValue::new("result", result)];

    i.requests_total.add(1, &attrs);
    i.latency_seconds.record(duration_seconds, &attrs);
}

pub fn record_query_bytes_scanned(bytes: u64) {
    let i = instruments();
    let attrs = common_attrs();
    i.bytes_scanned_total.add(bytes, &attrs);
}

pub fn record_cache_hit() {
    let i = instruments();
    let attrs = common_attrs();
    i.cache_hits_total.add(1, &attrs);
}

pub fn record_cache_miss() {
    let i = instruments();
    let attrs = common_attrs();
    i.cache_misses_total.add(1, &attrs);
}
