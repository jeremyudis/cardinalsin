//! Quantitative production-readiness benchmarks for the CSI inverted index.
//!
//! Measures:
//!   1. Build latency/throughput at varying rowcount × cardinality
//!   2. Segment byte size / compression ratio vs. raw term cost
//!   3. Pruning latency at varying segment count per shard
//!   4. Single-chunk-per-segment overhead vs. multi-chunk segment
//!   5. Lookup latency vs. naive HashMap baseline
//!   6. K-way merge cost at varying source segment count

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cardinalsin::index::{
    fst_builder::FstTermBuilder, segment::ChunkOrdinalTable, segment::SegmentReader,
    segment::SegmentWriter, IndexConfig, SegmentMerger,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;
use std::sync::Arc;

// ────────────────────────────────────────────────────────────────────────────
// Data generators
// ────────────────────────────────────────────────────────────────────────────

/// Deterministic batch with `rows` rows, `card` distinct host values and
/// `card/4` distinct region values.
fn make_batch(rows: usize, card: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
    ]));

    let hosts: Vec<String> = (0..rows).map(|i| format!("host-{:05}", i % card)).collect();
    let regions: Vec<String> = (0..rows)
        .map(|i| format!("region-{}", i % (card.max(4) / 4)))
        .collect();
    let services: Vec<String> = (0..rows)
        .map(|i| format!("svc-{}", i % 20)) // fixed low cardinality
        .collect();

    let host_ref: Vec<&str> = hosts.iter().map(String::as_str).collect();
    let region_ref: Vec<&str> = regions.iter().map(String::as_str).collect();
    let service_ref: Vec<&str> = services.iter().map(String::as_str).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(host_ref)),
            Arc::new(StringArray::from(region_ref)),
            Arc::new(StringArray::from(service_ref)),
        ],
    )
    .unwrap()
}

fn default_config() -> IndexConfig {
    IndexConfig {
        skip_columns: vec![],
        max_cardinality_for_inverted: 1_000_000,
        ..IndexConfig::default()
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 1. Build latency — rows × cardinality matrix
// ────────────────────────────────────────────────────────────────────────────

fn bench_build(c: &mut Criterion) {
    let mut g = c.benchmark_group("csi_build");
    for &rows in &[10_000usize, 100_000, 1_000_000] {
        for &card in &[10usize, 1_000, 10_000] {
            if card > rows {
                continue;
            }
            let batch = make_batch(rows, card);
            let ordinals = vec![0u32; rows];
            let cfg = default_config();
            g.throughput(Throughput::Elements(rows as u64));
            g.bench_with_input(
                BenchmarkId::new(format!("rows={}", rows), format!("card={}", card)),
                &batch,
                |b, batch| {
                    b.iter(|| {
                        let cols = FstTermBuilder::build_all_columns(
                            batch,
                            &ordinals,
                            &cfg.skip_columns,
                            cfg.max_cardinality_for_inverted,
                        )
                        .unwrap();
                        let table = ChunkOrdinalTable::new(vec![(0, "chunk".into())]);
                        let bytes = SegmentWriter::write_segment(&table, &cols).unwrap();
                        black_box(bytes);
                    });
                },
            );
        }
    }
    g.finish();
}

// ────────────────────────────────────────────────────────────────────────────
// 2. Segment size efficiency — bytes per term, vs. minimum theoretical
// ────────────────────────────────────────────────────────────────────────────

fn bench_size(c: &mut Criterion) {
    let mut g = c.benchmark_group("csi_size_bytes_per_term");
    g.sample_size(10);
    for &card in &[10usize, 100, 1_000, 10_000] {
        let rows = 100_000.max(card);
        let batch = make_batch(rows, card);
        let ordinals = vec![0u32; rows];
        let cfg = default_config();
        let cols = FstTermBuilder::build_all_columns(
            &batch,
            &ordinals,
            &cfg.skip_columns,
            cfg.max_cardinality_for_inverted,
        )
        .unwrap();
        let table = ChunkOrdinalTable::new(vec![(0, "chunk-path/1.parquet".into())]);
        let seg = SegmentWriter::write_segment(&table, &cols).unwrap();
        let total_terms: usize = cols.iter().map(|c| c.postings.len()).sum();
        let bytes_per_term = seg.len() as f64 / total_terms.max(1) as f64;
        eprintln!(
            "[csi_size] card={:>5}  total_terms={:>6}  segment_bytes={:>9}  bytes/term={:.1}",
            card,
            total_terms,
            seg.len(),
            bytes_per_term
        );
        // No-op bench, just record the number
        g.bench_with_input(BenchmarkId::new("card", card), &seg, |b, seg| {
            b.iter(|| black_box(seg.len()));
        });
    }
    g.finish();
}

// ────────────────────────────────────────────────────────────────────────────
// 3. Lookup latency — FST+Roaring vs. HashMap baseline
// ────────────────────────────────────────────────────────────────────────────

/// Naive baseline: HashMap<String, HashMap<String, Vec<u32>>> — column → value → ordinals.
struct NaiveIndex {
    data: HashMap<String, HashMap<String, Vec<u32>>>,
}

impl NaiveIndex {
    fn build(batch: &RecordBatch, ordinals: &[u32]) -> Self {
        let mut data: HashMap<String, HashMap<String, Vec<u32>>> = HashMap::new();
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let arr = batch
                .column(col_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let map = data.entry(field.name().clone()).or_default();
            for (row, ord) in ordinals.iter().enumerate() {
                let v = arr.value(row).to_string();
                map.entry(v).or_default().push(*ord);
            }
        }
        Self { data }
    }

    fn lookup(&self, col: &str, val: &str) -> Option<&[u32]> {
        self.data.get(col)?.get(val).map(|v| v.as_slice())
    }

    fn size_bytes(&self) -> usize {
        // rough: sum of string bytes + vec bytes
        self.data
            .iter()
            .map(|(k, m)| {
                k.len()
                    + m.iter()
                        .map(|(v, ords)| v.len() + ords.len() * 4 + 16)
                        .sum::<usize>()
            })
            .sum()
    }
}

fn bench_lookup(c: &mut Criterion) {
    let mut g = c.benchmark_group("csi_lookup_latency");
    for &card in &[100usize, 10_000] {
        let rows = 100_000;
        let batch = make_batch(rows, card);
        let ordinals = vec![0u32; rows];
        let cfg = default_config();
        let cols = FstTermBuilder::build_all_columns(
            &batch,
            &ordinals,
            &cfg.skip_columns,
            cfg.max_cardinality_for_inverted,
        )
        .unwrap();
        let table = ChunkOrdinalTable::new(vec![(0, "chunk".into())]);
        let seg_bytes = SegmentWriter::write_segment(&table, &cols).unwrap();
        let csi_size = seg_bytes.len();
        let reader = SegmentReader::open(seg_bytes).unwrap();

        let naive = NaiveIndex::build(&batch, &ordinals);
        let naive_size = naive.size_bytes();

        eprintln!(
            "[csi_vs_naive] card={:>5}  csi_bytes={:>8}  naive_bytes={:>8}  ratio={:.2}x",
            card,
            csi_size,
            naive_size,
            naive_size as f64 / csi_size as f64
        );

        let target = format!("host-{:05}", card / 2);
        g.bench_with_input(BenchmarkId::new("csi_hit", card), &reader, |b, reader| {
            b.iter(|| {
                let r = reader.lookup("host", &target).unwrap();
                black_box(r);
            });
        });
        g.bench_with_input(BenchmarkId::new("csi_miss", card), &reader, |b, reader| {
            b.iter(|| {
                let r = reader.lookup("host", "NONEXISTENT").unwrap();
                black_box(r);
            });
        });
        g.bench_with_input(BenchmarkId::new("naive_hit", card), &naive, |b, naive| {
            b.iter(|| {
                let r = naive.lookup("host", &target);
                black_box(r);
            });
        });
    }
    g.finish();
}

// ────────────────────────────────────────────────────────────────────────────
// 4. K-way merge cost — merge latency vs. source segment count
// ────────────────────────────────────────────────────────────────────────────

fn bench_merge(c: &mut Criterion) {
    let mut g = c.benchmark_group("csi_merge");
    g.sample_size(10);
    for &n_segments in &[2usize, 8, 32, 128] {
        // Each segment simulates one flushed chunk at 10k rows, 500 distinct hosts
        let rows = 10_000;
        let card = 500;
        let batch = make_batch(rows, card);
        let cfg = default_config();

        let source_bytes: Vec<Vec<u8>> = (0..n_segments)
            .map(|i| {
                let ordinals = vec![0u32; rows];
                let cols = FstTermBuilder::build_all_columns(
                    &batch,
                    &ordinals,
                    &cfg.skip_columns,
                    cfg.max_cardinality_for_inverted,
                )
                .unwrap();
                let table = ChunkOrdinalTable::new(vec![(0, format!("chunk-{}.parquet", i))]);
                SegmentWriter::write_segment(&table, &cols).unwrap()
            })
            .collect();

        let total_src_bytes: usize = source_bytes.iter().map(|b| b.len()).sum();
        let output_paths: Vec<String> = (0..n_segments)
            .map(|i| format!("chunk-{}.parquet", i))
            .collect();
        // Identity remap: each source path keeps its position in the output.
        let path_remap: std::collections::HashMap<String, u32> = output_paths
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), i as u32))
            .collect();

        g.throughput(Throughput::Bytes(total_src_bytes as u64));
        g.bench_with_input(
            BenchmarkId::new("n_segments", n_segments),
            &source_bytes,
            |b, source_bytes| {
                b.iter(|| {
                    let readers: Vec<SegmentReader> = source_bytes
                        .iter()
                        .map(|bytes| SegmentReader::open(bytes.clone()).unwrap())
                        .collect();
                    let merged =
                        SegmentMerger::merge_segments(&readers, &output_paths, &path_remap)
                            .unwrap();
                    black_box(merged);
                });
            },
        );
    }
    g.finish();
}

// ────────────────────────────────────────────────────────────────────────────
// 5. Single-chunk-per-segment overhead vs. a single batched segment
// ────────────────────────────────────────────────────────────────────────────

fn bench_single_vs_batched(c: &mut Criterion) {
    let mut g = c.benchmark_group("csi_single_vs_batched");
    g.sample_size(10);

    // Scenario: 100 chunks, each with 10k rows, 500 hosts distinct per chunk,
    //   overlapping global term space (total 5000 distinct hosts).
    let n_chunks = 100usize;
    let rows_per_chunk = 10_000usize;
    let card_per_chunk = 500usize;
    let cfg = default_config();

    // Option A: current design — one .csi per chunk
    let batches: Vec<RecordBatch> = (0..n_chunks)
        .map(|_| make_batch(rows_per_chunk, card_per_chunk))
        .collect();
    let mut total_a_bytes = 0usize;
    let mut total_a_build_ns = 0u128;
    for (i, batch) in batches.iter().enumerate() {
        let t0 = std::time::Instant::now();
        let ordinals = vec![0u32; rows_per_chunk];
        let cols = FstTermBuilder::build_all_columns(
            batch,
            &ordinals,
            &cfg.skip_columns,
            cfg.max_cardinality_for_inverted,
        )
        .unwrap();
        let table = ChunkOrdinalTable::new(vec![(0, format!("chunk-{}.parquet", i))]);
        let bytes = SegmentWriter::write_segment(&table, &cols).unwrap();
        total_a_build_ns += t0.elapsed().as_nanos();
        total_a_bytes += bytes.len();
    }

    // Option B: batched — one .csi for all 100 chunks, with proper ordinals
    let t0 = std::time::Instant::now();
    // Build a merged batch: concatenate by ordinal — for build_all_columns
    // we simulate by running through the same batch 100x with different ordinals.
    // Since build_all_columns works on a single RecordBatch, do proper concatenation.
    use arrow::compute::concat_batches;
    let schema = batches[0].schema();
    let merged_batch = concat_batches(&schema, &batches).unwrap();
    let mut ordinals_b = Vec::with_capacity(merged_batch.num_rows());
    for i in 0..n_chunks {
        for _ in 0..rows_per_chunk {
            ordinals_b.push(i as u32);
        }
    }
    let cols_b = FstTermBuilder::build_all_columns(
        &merged_batch,
        &ordinals_b,
        &cfg.skip_columns,
        cfg.max_cardinality_for_inverted,
    )
    .unwrap();
    let table_b = ChunkOrdinalTable::new(
        (0..n_chunks)
            .map(|i| (i as u32, format!("chunk-{}.parquet", i)))
            .collect(),
    );
    let bytes_b = SegmentWriter::write_segment(&table_b, &cols_b).unwrap();
    let total_b_build_ns = t0.elapsed().as_nanos();
    let total_b_bytes = bytes_b.len();

    eprintln!(
        "[single_vs_batched] n_chunks={}  single: bytes={} build_ms={:.1}   batched: bytes={} build_ms={:.1}   size_ratio={:.2}x",
        n_chunks,
        total_a_bytes,
        total_a_build_ns as f64 / 1e6,
        total_b_bytes,
        total_b_build_ns as f64 / 1e6,
        total_a_bytes as f64 / total_b_bytes as f64,
    );

    // Also measure total-lookup-cost across 100 segments vs. 1 batched segment.
    let readers_a: Vec<SegmentReader> = (0..n_chunks)
        .map(|i| {
            let ordinals = vec![0u32; rows_per_chunk];
            let cols = FstTermBuilder::build_all_columns(
                &batches[i],
                &ordinals,
                &cfg.skip_columns,
                cfg.max_cardinality_for_inverted,
            )
            .unwrap();
            let table = ChunkOrdinalTable::new(vec![(0, format!("chunk-{}.parquet", i))]);
            let bytes = SegmentWriter::write_segment(&table, &cols).unwrap();
            SegmentReader::open(bytes).unwrap()
        })
        .collect();
    let reader_b = SegmentReader::open(bytes_b).unwrap();

    let target = format!("host-{:05}", card_per_chunk / 2);

    g.bench_function("100_segments_lookup_all", |b| {
        b.iter(|| {
            let mut hits = 0u32;
            for r in &readers_a {
                if let Some(pl) = r.lookup("host", &target).unwrap() {
                    hits += pl.len() as u32;
                }
            }
            black_box(hits);
        });
    });
    g.bench_function("1_batched_segment_lookup", |b| {
        b.iter(|| {
            let pl = reader_b.lookup("host", &target).unwrap().unwrap();
            black_box(pl.len());
        });
    });
    g.finish();
}

// ────────────────────────────────────────────────────────────────────────────
// 6. CRC overhead — measure the cost of per-lookup CRC validation
// ────────────────────────────────────────────────────────────────────────────

fn bench_crc_overhead(c: &mut Criterion) {
    let mut g = c.benchmark_group("csi_crc_overhead");
    for &card in &[1_000usize, 10_000, 100_000] {
        let rows = card.max(10_000);
        let batch = make_batch(rows, card);
        let ordinals = vec![0u32; rows];
        let cfg = default_config();
        let cols = FstTermBuilder::build_all_columns(
            &batch,
            &ordinals,
            &cfg.skip_columns,
            cfg.max_cardinality_for_inverted,
        )
        .unwrap();
        let table = ChunkOrdinalTable::new(vec![(0, "chunk".into())]);
        let seg_bytes = SegmentWriter::write_segment(&table, &cols).unwrap();
        let reader = SegmentReader::open(seg_bytes.clone()).unwrap();
        let target = format!("host-{:05}", card / 2);

        g.bench_with_input(
            BenchmarkId::new("lookup_with_crc", card),
            &reader,
            |b, r| {
                b.iter(|| {
                    let result = r.lookup("host", &target).unwrap();
                    black_box(result);
                });
            },
        );
        // Also measure raw CRC on full segment
        let seg_for_crc = seg_bytes.clone();
        g.bench_with_input(
            BenchmarkId::new("raw_crc32_full_segment", card),
            &seg_for_crc,
            |b, s| {
                b.iter(|| black_box(crc32fast::hash(s)));
            },
        );
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_build,
    bench_size,
    bench_lookup,
    bench_merge,
    bench_single_vs_batched,
    bench_crc_overhead,
);
criterion_main!(benches);
