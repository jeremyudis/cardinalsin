//! Write throughput benchmark

use cardinalsin::ingester::ParquetWriter;

use arrow_array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
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
        Field::new("host", DataType::Utf8, true),
        Field::new("service", DataType::Utf8, true),
    ]));

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let timestamps: Vec<i64> = (0..rows as i64).map(|i| now + i * 1_000_000).collect();
    let names: Vec<&str> = (0..rows).map(|_| "cpu_usage").collect();
    let values: Vec<f64> = (0..rows).map(|i| (i as f64 % 100.0) / 100.0).collect();
    let hosts: Vec<&str> = (0..rows)
        .map(|i| match i % 10 {
            0 => "server-01",
            1 => "server-02",
            2 => "server-03",
            3 => "server-04",
            4 => "server-05",
            5 => "server-06",
            6 => "server-07",
            7 => "server-08",
            8 => "server-09",
            _ => "server-10",
        })
        .collect();
    let services: Vec<&str> = (0..rows)
        .map(|i| match i % 5 {
            0 => "api-gateway",
            1 => "auth-service",
            2 => "user-service",
            3 => "order-service",
            _ => "payment-service",
        })
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(hosts)),
            Arc::new(StringArray::from(services)),
        ],
    )
    .unwrap()
}

fn benchmark_parquet_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_write");

    for rows in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(rows as u64));

        let batch = create_test_batch(rows);
        let writer = ParquetWriter::new();

        group.bench_function(format!("{}_rows", rows), |b| {
            b.iter(|| {
                let _ = black_box(writer.write_batch(&batch).unwrap());
            });
        });
    }

    group.finish();
}

fn benchmark_buffer_append(c: &mut Criterion) {
    use cardinalsin::ingester::WriteBuffer;

    let mut group = c.benchmark_group("buffer_append");

    for rows in [1_000, 10_000] {
        group.throughput(Throughput::Elements(rows as u64));

        let batch = create_test_batch(rows);

        group.bench_function(format!("{}_rows", rows), |b| {
            b.iter(|| {
                let mut buffer = WriteBuffer::new();
                buffer.append(black_box(batch.clone())).unwrap();
            });
        });
    }

    group.finish();
}

fn benchmark_compression_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    for rows in [10_000, 100_000] {
        let batch = create_test_batch(rows);
        let writer = ParquetWriter::new();

        let uncompressed_size = batch.get_array_memory_size();
        let compressed = writer.write_batch(&batch).unwrap();
        let ratio = uncompressed_size as f64 / compressed.len() as f64;

        println!(
            "Rows: {}, Uncompressed: {} bytes, Compressed: {} bytes, Ratio: {:.2}x",
            rows,
            uncompressed_size,
            compressed.len(),
            ratio
        );

        group.bench_function(format!("{}_rows", rows), |b| {
            b.iter(|| {
                let _ = black_box(writer.write_batch(&batch).unwrap());
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_parquet_write,
    benchmark_buffer_append,
    benchmark_compression_ratio,
);

criterion_main!(benches);
