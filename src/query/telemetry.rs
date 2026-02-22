//! Query-path telemetry instruments and recording helpers.

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;
use std::sync::OnceLock;

struct QueryInstruments {
    query_requests: Counter<u64>,
    query_duration_seconds: Histogram<f64>,
    query_rows_returned: Histogram<u64>,
    query_bytes_scanned: Histogram<u64>,
    query_bytes_returned: Histogram<u64>,
    query_pruning_ratio: Histogram<f64>,
    query_chunks_selected: Histogram<u64>,
    query_chunks_candidate: Histogram<u64>,
    chunk_registration_outcomes: Counter<u64>,
    cache_hits: Counter<u64>,
    cache_misses: Counter<u64>,
    cache_evictions: Counter<u64>,
    cache_size_bytes: Histogram<u64>,
}

fn instruments() -> &'static QueryInstruments {
    static INSTRUMENTS: OnceLock<QueryInstruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("cardinalsin.query");
        QueryInstruments {
            query_requests: meter
                .u64_counter("cardinalsin.query.requests")
                .with_description("Total query requests by outcome")
                .init(),
            query_duration_seconds: meter
                .f64_histogram("cardinalsin.query.duration")
                .with_description("Query end-to-end latency")
                .with_unit("s")
                .init(),
            query_rows_returned: meter
                .u64_histogram("cardinalsin.query.rows_returned")
                .with_description("Rows returned per query")
                .init(),
            query_bytes_scanned: meter
                .u64_histogram("cardinalsin.query.bytes_scanned")
                .with_description("Estimated bytes scanned from selected chunks")
                .with_unit("By")
                .init(),
            query_bytes_returned: meter
                .u64_histogram("cardinalsin.query.bytes_returned")
                .with_description("Bytes in returned Arrow batches")
                .with_unit("By")
                .init(),
            query_pruning_ratio: meter
                .f64_histogram("cardinalsin.query.metadata_pruning_ratio")
                .with_description("Metadata pruning effectiveness ratio")
                .init(),
            query_chunks_selected: meter
                .u64_histogram("cardinalsin.query.chunks_selected")
                .with_description("Chunks selected after metadata pruning")
                .init(),
            query_chunks_candidate: meter
                .u64_histogram("cardinalsin.query.chunks_candidate")
                .with_description("Candidate chunks before metadata pruning")
                .init(),
            chunk_registration_outcomes: meter
                .u64_counter("cardinalsin.query.chunk_registration.outcomes")
                .with_description("Chunk registration outcomes in DataFusion context")
                .init(),
            cache_hits: meter
                .u64_counter("cardinalsin.query.cache.hits")
                .with_description("Cache hits observed at access time")
                .init(),
            cache_misses: meter
                .u64_counter("cardinalsin.query.cache.misses")
                .with_description("Cache misses observed at access time")
                .init(),
            cache_evictions: meter
                .u64_counter("cardinalsin.query.cache.evictions")
                .with_description("Cache evictions observed at removal time")
                .init(),
            cache_size_bytes: meter
                .u64_histogram("cardinalsin.query.cache.size_bytes")
                .with_description("Cache size snapshots")
                .with_unit("By")
                .init(),
        }
    })
}

pub fn record_chunk_registration(outcome: &'static str) {
    instruments()
        .chunk_registration_outcomes
        .add(1, &[KeyValue::new("outcome", outcome)]);
}

pub struct QueryMetrics {
    pub outcome: &'static str,
    pub error_class: Option<&'static str>,
    pub duration_seconds: f64,
    pub rows_returned: u64,
    pub bytes_scanned: u64,
    pub bytes_returned: u64,
    pub chunks_selected: u64,
    pub chunks_candidate: u64,
}

pub fn record_query(metrics: QueryMetrics) {
    let i = instruments();
    let mut attrs = vec![KeyValue::new("outcome", metrics.outcome)];
    if let Some(error_class) = metrics.error_class {
        attrs.push(KeyValue::new("error.class", error_class));
    }

    i.query_requests.add(1, &attrs);
    i.query_duration_seconds
        .record(metrics.duration_seconds, &attrs);
    i.query_rows_returned.record(metrics.rows_returned, &attrs);
    i.query_bytes_scanned.record(metrics.bytes_scanned, &attrs);
    i.query_bytes_returned
        .record(metrics.bytes_returned, &attrs);
    i.query_chunks_selected
        .record(metrics.chunks_selected, &attrs);
    i.query_chunks_candidate
        .record(metrics.chunks_candidate, &attrs);

    let pruning_ratio = if metrics.chunks_candidate == 0 {
        0.0
    } else {
        (1.0 - (metrics.chunks_selected as f64 / metrics.chunks_candidate as f64)).clamp(0.0, 1.0)
    };
    i.query_pruning_ratio.record(pruning_ratio, &attrs);
}

pub fn record_cache_hit(tier: &'static str) {
    instruments()
        .cache_hits
        .add(1, &[KeyValue::new("tier", tier)]);
}

pub fn record_cache_miss(tier: &'static str) {
    instruments()
        .cache_misses
        .add(1, &[KeyValue::new("tier", tier)]);
}

pub fn record_cache_eviction(tier: &'static str) {
    instruments()
        .cache_evictions
        .add(1, &[KeyValue::new("tier", tier)]);
}

pub fn record_cache_size_snapshot(l1_size_bytes: usize, l2_size_bytes: usize) {
    let i = instruments();
    i.cache_size_bytes
        .record(l1_size_bytes as u64, &[KeyValue::new("tier", "l1")]);
    i.cache_size_bytes
        .record(l2_size_bytes as u64, &[KeyValue::new("tier", "l2")]);
}
