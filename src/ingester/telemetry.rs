//! Ingester telemetry instruments and recording helpers.

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;
use std::sync::OnceLock;

struct IngesterInstruments {
    write_requests: Counter<u64>,
    write_bytes: Counter<u64>,
    write_duration_seconds: Histogram<f64>,
    write_batch_size_bytes: Histogram<u64>,
    buffer_fullness_ratio: Histogram<f64>,
    backpressure_rejections: Counter<u64>,
    flush_triggers: Counter<u64>,
    flush_duration_seconds: Histogram<f64>,
    flush_bytes: Histogram<u64>,
    flush_rows: Histogram<u64>,
    wal_append_outcomes: Counter<u64>,
    wal_replay_entries: Counter<u64>,
    wal_truncate_outcomes: Counter<u64>,
    split_dual_write_requests: Counter<u64>,
}

fn instruments() -> &'static IngesterInstruments {
    static INSTRUMENTS: OnceLock<IngesterInstruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("cardinalsin.ingester");
        IngesterInstruments {
            write_requests: meter
                .u64_counter("cardinalsin.ingester.write.requests")
                .with_description("Total write requests handled by ingester")
                .init(),
            write_bytes: meter
                .u64_counter("cardinalsin.ingester.write.bytes")
                .with_description("Total bytes accepted by ingester write path")
                .with_unit("By")
                .init(),
            write_duration_seconds: meter
                .f64_histogram("cardinalsin.ingester.write.duration")
                .with_description("Ingester write duration")
                .with_unit("s")
                .init(),
            write_batch_size_bytes: meter
                .u64_histogram("cardinalsin.ingester.write.batch_size")
                .with_description("Write batch payload size")
                .with_unit("By")
                .init(),
            buffer_fullness_ratio: meter
                .f64_histogram("cardinalsin.ingester.buffer.fullness_ratio")
                .with_description("Write buffer fullness ratio sampled at write time")
                .init(),
            backpressure_rejections: meter
                .u64_counter("cardinalsin.ingester.backpressure.rejections")
                .with_description("Writes rejected because buffer is full")
                .init(),
            flush_triggers: meter
                .u64_counter("cardinalsin.ingester.flush.triggers")
                .with_description("Flush trigger counts by reason")
                .init(),
            flush_duration_seconds: meter
                .f64_histogram("cardinalsin.ingester.flush.duration")
                .with_description("Flush operation duration")
                .with_unit("s")
                .init(),
            flush_bytes: meter
                .u64_histogram("cardinalsin.ingester.flush.bytes")
                .with_description("Bytes written in each flush")
                .with_unit("By")
                .init(),
            flush_rows: meter
                .u64_histogram("cardinalsin.ingester.flush.rows")
                .with_description("Rows written in each flush")
                .init(),
            wal_append_outcomes: meter
                .u64_counter("cardinalsin.ingester.wal.append.outcomes")
                .with_description("WAL append outcomes")
                .init(),
            wal_replay_entries: meter
                .u64_counter("cardinalsin.ingester.wal.replay.entries")
                .with_description("WAL entries replayed during startup recovery")
                .init(),
            wal_truncate_outcomes: meter
                .u64_counter("cardinalsin.ingester.wal.truncate.outcomes")
                .with_description("WAL truncate outcomes after flush")
                .init(),
            split_dual_write_requests: meter
                .u64_counter("cardinalsin.ingester.split.dual_write.requests")
                .with_description("Write requests routed through split-aware dual-write path")
                .init(),
        }
    })
}

pub fn record_write(duration_seconds: f64, batch_size_bytes: u64, buffer_fullness_ratio: f64) {
    let i = instruments();
    i.write_requests.add(1, &[]);
    i.write_bytes.add(batch_size_bytes, &[]);
    i.write_duration_seconds.record(duration_seconds, &[]);
    i.write_batch_size_bytes.record(batch_size_bytes, &[]);
    i.buffer_fullness_ratio
        .record(buffer_fullness_ratio.clamp(0.0, 1.0), &[]);
}

pub fn record_backpressure_rejection() {
    instruments().backpressure_rejections.add(1, &[]);
}

pub fn record_flush_trigger(reason: &'static str) {
    instruments()
        .flush_triggers
        .add(1, &[KeyValue::new("reason", reason)]);
}

pub fn record_flush(duration_seconds: f64, bytes: u64, rows: u64) {
    let i = instruments();
    i.flush_duration_seconds.record(duration_seconds, &[]);
    i.flush_bytes.record(bytes, &[]);
    i.flush_rows.record(rows, &[]);
}

pub fn record_wal_append_outcome(outcome: &'static str) {
    instruments()
        .wal_append_outcomes
        .add(1, &[KeyValue::new("outcome", outcome)]);
}

pub fn record_wal_replay_entries(entries: u64) {
    instruments().wal_replay_entries.add(entries, &[]);
}

pub fn record_wal_truncate_outcome(outcome: &'static str) {
    instruments()
        .wal_truncate_outcomes
        .add(1, &[KeyValue::new("outcome", outcome)]);
}

pub fn record_split_dual_write_request() {
    instruments().split_dual_write_requests.add(1, &[]);
}
