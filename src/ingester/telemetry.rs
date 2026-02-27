//! Ingester telemetry instruments and recording helpers.

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::KeyValue;
use std::sync::OnceLock;

struct IngesterInstruments {
    write_rows_total: Counter<u64>,
    write_latency_seconds: Histogram<f64>,
    wal_operations_total: Counter<u64>,
    buffer_fullness_ratio: Gauge<f64>,
}

fn instruments() -> &'static IngesterInstruments {
    static INSTRUMENTS: OnceLock<IngesterInstruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("cardinalsin.ingester");
        IngesterInstruments {
            write_rows_total: meter
                .u64_counter("cardinalsin_ingester_write_rows_total")
                .with_description("Rows handled by ingester writes")
                .with_unit("1")
                .init(),
            write_latency_seconds: meter
                .f64_histogram("cardinalsin_ingester_write_latency_seconds")
                .with_description("End-to-end ingester write latency")
                .with_unit("s")
                .init(),
            wal_operations_total: meter
                .u64_counter("cardinalsin_ingester_wal_operations_total")
                .with_description("WAL operation outcomes in ingester")
                .with_unit("1")
                .init(),
            buffer_fullness_ratio: meter
                .f64_gauge("cardinalsin_ingester_buffer_fullness_ratio")
                .with_description("Current ingester write buffer fullness ratio")
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

pub fn record_write(rows: u64, duration_seconds: f64, result: &'static str) {
    let i = instruments();
    let [service, run_id, tenant] = common_attrs();
    let attrs = [service, run_id, tenant, KeyValue::new("result", result)];

    i.write_rows_total.add(rows, &attrs);
    i.write_latency_seconds.record(duration_seconds, &attrs);
}

pub fn record_wal_operation(operation: &'static str, result: &'static str) {
    let i = instruments();
    let [service, run_id, tenant] = common_attrs();
    let attrs = [
        service,
        run_id,
        tenant,
        KeyValue::new("operation", operation),
        KeyValue::new("result", result),
    ];
    i.wal_operations_total.add(1, &attrs);
}

pub fn record_buffer_fullness_ratio(fullness_ratio: f64) {
    let i = instruments();
    let attrs = common_attrs();
    i.buffer_fullness_ratio
        .record(fullness_ratio.clamp(0.0, 1.0), &attrs);
}
