//! Query latency benchmark

use cardinalsin::query::{TieredCache, CacheConfig};

use arrow_array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

fn create_test_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value_f64", DataType::Float64, true),
    ]));

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let timestamps: Vec<i64> = (0..rows as i64).map(|i| now + i * 1_000_000).collect();
    let names: Vec<&str> = (0..rows).map(|_| "cpu_usage").collect();
    let values: Vec<f64> = (0..rows).map(|i| (i as f64 % 100.0) / 100.0).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

fn create_parquet_bytes(rows: usize) -> Bytes {
    let batch = create_test_batch(rows);
    let mut buffer = Vec::new();

    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    Bytes::from(buffer)
}

fn benchmark_cache_hit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("cache_hit");
    group.throughput(Throughput::Elements(1));

    let cache = TieredCache::new(CacheConfig::default()).unwrap();
    let data = create_parquet_bytes(10_000);

    // Pre-populate cache
    rt.block_on(async {
        cache.insert("test_key", data.clone()).await;
    });

    group.bench_function("l1_hit", |b| {
        b.to_async(&rt).iter(|| async {
            let result = cache.get_or_fetch("test_key", || async {
                panic!("Should not fetch");
            }).await.unwrap();
            black_box(result);
        });
    });

    group.finish();
}

fn benchmark_cache_miss(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("cache_miss");
    group.throughput(Throughput::Elements(1));

    let data = create_parquet_bytes(10_000);

    group.bench_function("fetch_and_cache", |b| {
        b.to_async(&rt).iter_batched(
            || TieredCache::new(CacheConfig::default()).unwrap(),
            |cache| async {
                let data = data.clone();
                let result = cache.get_or_fetch("test_key", || async {
                    Ok(data)
                }).await.unwrap();
                black_box(result);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_cache_hit,
    benchmark_cache_miss,
);

criterion_main!(benches);
