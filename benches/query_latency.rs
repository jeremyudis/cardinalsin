//! Query latency benchmark

use cardinalsin::query::{TieredCache, CacheConfig};

use arrow_array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use parquet::arrow::ArrowWriter;
use std::sync::Arc;
use tempfile::tempdir;

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

fn create_cache_config() -> (CacheConfig, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let config = CacheConfig {
        l1_size: 100 * 1024 * 1024, // 100MB
        l2_size: 500 * 1024 * 1024, // 500MB
        l2_dir: Some(dir.path().to_str().unwrap().to_string()),
    };
    (config, dir)
}

fn benchmark_cache_hit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("cache_hit");
    group.throughput(Throughput::Elements(1));

    let (config, _dir) = create_cache_config();
    let data = create_parquet_bytes(10_000);

    // Create cache and pre-populate it via get_or_fetch
    let cache = rt.block_on(async {
        let cache = TieredCache::new(config).await.unwrap();
        let data_clone = data.clone();
        // Pre-populate cache by fetching once
        let _ = cache.get_or_fetch("test_key", || async move {
            Ok(data_clone)
        }).await.unwrap();
        cache
    });

    group.bench_function("l1_hit", |b| {
        b.iter(|| {
            let result = rt.block_on(async {
                cache
                    .get_or_fetch("test_key", || async {
                        panic!("Should not fetch - cache should be populated");
                    })
                    .await
                    .unwrap()
            });
            black_box(result);
        });
    });

    // Shutdown L2 background workers before dropping the runtime.
    rt.block_on(async {
        cache.clear().await;
    });
    group.finish();
}

fn benchmark_cache_miss(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("cache_miss");
    group.throughput(Throughput::Elements(1));

    let (config, _dir) = create_cache_config();
    let cache = rt.block_on(async { TieredCache::new(config).await.unwrap() });
    let data = create_parquet_bytes(10_000);

    group.bench_function("fetch_and_cache", |b| {
        let mut key_idx = 0u64;
        b.iter(|| {
            // Force a miss by using a fresh key each iteration.
            let key = format!("test_key_{}", key_idx);
            key_idx += 1;
            let data_clone = data.clone();
            let result = rt.block_on(async {
                cache
                    .get_or_fetch(&key, || async move { Ok(data_clone) })
                    .await
                    .unwrap()
            });
            black_box(result);
        });
    });

    rt.block_on(async {
        cache.clear().await;
    });
    group.finish();
}

criterion_group!(
    benches,
    benchmark_cache_hit,
    benchmark_cache_miss,
);

criterion_main!(benches);
