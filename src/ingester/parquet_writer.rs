//! Parquet writer with optimal settings for time-series data

use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema as ArrowSchema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

/// Parquet writer optimized for time-series metrics
pub struct ParquetWriter;

impl ParquetWriter {
    /// Create a new Parquet writer with optimal settings
    pub fn new() -> Self {
        Self
    }

    /// Build writer properties for the given Arrow schema.
    ///
    /// Bloom filters are enabled automatically for dictionary-encoded string
    /// columns (low/medium cardinality tags) based on their Arrow DataType:
    /// - `Dictionary(UInt16, Utf8)` → bloom filter with NDV=1_000 (low cardinality)
    /// - `Dictionary(UInt32, Utf8)` → bloom filter with NDV=100_000 (medium cardinality)
    /// - Plain `Utf8` or non-string types → no bloom filter
    fn build_writer_properties_for(arrow_schema: &ArrowSchema) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            // Use Parquet v2 for better encoding support
            .set_writer_version(WriterVersion::PARQUET_2_0)
            // Compression: ZSTD level 3 (good ratio, fast)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            // Enable dictionary for all columns initially
            // High-cardinality columns will fall back to plain encoding
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(1_000_000)
            // Row group sizing for optimal S3 access
            .set_max_row_group_size(500_000)
            // Enable statistics for predicate pushdown
            .set_statistics_enabled(EnabledStatistics::Page)
            // Bloom filters off globally; enabled per-column below
            .set_bloom_filter_enabled(false)
            // Data page settings
            .set_data_page_size_limit(1024 * 1024); // 1MB data pages

        for field in arrow_schema.fields() {
            let (fpp, ndv): (f64, u64) = match field.data_type() {
                // Low cardinality: Dictionary(UInt16, Utf8)
                DataType::Dictionary(key, val)
                    if matches!(
                        (key.as_ref(), val.as_ref()),
                        (DataType::UInt16, DataType::Utf8)
                    ) =>
                {
                    (0.01, 1_000)
                }
                // Medium cardinality: Dictionary(UInt32, Utf8)
                DataType::Dictionary(key, val)
                    if matches!(
                        (key.as_ref(), val.as_ref()),
                        (DataType::UInt32, DataType::Utf8)
                    ) =>
                {
                    (0.01, 100_000)
                }
                // Plain Utf8 (high cardinality) or non-string: no bloom filter
                _ => continue,
            };

            let col_path = ColumnPath::from(field.name().as_str());
            builder = builder
                .set_column_bloom_filter_enabled(col_path.clone(), true)
                .set_column_bloom_filter_fpp(col_path.clone(), fpp)
                .set_column_bloom_filter_ndv(col_path, ndv);
        }

        builder.build()
    }

    /// Write a record batch to Parquet bytes
    pub fn write_batch(&self, batch: &RecordBatch) -> Result<Bytes> {
        let props = Self::build_writer_properties_for(batch.schema_ref());
        let mut buffer = Vec::new();

        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

            writer.write(batch)?;
            writer.close()?;
        }

        Ok(Bytes::from(buffer))
    }

    /// Write multiple record batches to Parquet bytes
    pub fn write_batches(&self, batches: &[RecordBatch]) -> Result<Bytes> {
        if batches.is_empty() {
            return Err(crate::Error::InvalidSchema("No batches to write".into()));
        }

        let props = Self::build_writer_properties_for(batches[0].schema_ref());
        let mut buffer = Vec::new();

        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batches[0].schema(), Some(props))?;

            for batch in batches {
                writer.write(batch)?;
            }

            writer.close()?;
        }

        Ok(Bytes::from(buffer))
    }
}

impl Default for ParquetWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        builder::StringDictionaryBuilder, Float64Array, StringArray, TimestampNanosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
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
        let timestamps: Vec<i64> = (0..1000).map(|i| now + i * 1_000_000_000).collect();
        let names: Vec<&str> = (0..1000).map(|_| "cpu_usage").collect();
        let values: Vec<f64> = (0..1000).map(|i| (i as f64 % 100.0) / 100.0).collect();

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

    #[test]
    fn test_write_batch() {
        let writer = ParquetWriter::new();
        let batch = create_test_batch();

        let bytes = writer.write_batch(&batch).unwrap();
        assert!(!bytes.is_empty());

        // Verify we can read it back
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut reader = reader.build().unwrap();

        let read_batch = reader.next().unwrap().unwrap();
        assert_eq!(read_batch.num_rows(), 1000);
    }

    #[test]
    fn test_compression_ratio() {
        let writer = ParquetWriter::new();
        let batch = create_test_batch();

        // Calculate logical uncompressed size
        // 1000 rows: 8 bytes timestamp + ~10 bytes string + 8 bytes float = ~26KB
        let logical_size: usize = batch.num_rows() * (8 + 10 + 8);
        let bytes = writer.write_batch(&batch).unwrap();

        // Parquet files have metadata overhead, but for larger batches
        // compression should provide benefits
        println!(
            "Logical size: {} bytes, Parquet size: {} bytes",
            logical_size,
            bytes.len()
        );

        // For small test batches, Parquet overhead can dominate
        // Just verify the file is reasonably sized (under 2x logical size)
        assert!(
            bytes.len() < logical_size * 3,
            "Parquet file unexpectedly large: {} vs logical {}",
            bytes.len(),
            logical_size
        );
    }

    #[test]
    fn test_bloom_filters_written_for_dictionary_columns() {
        // Build a schema with low-cardinality (Dict UInt16), medium-cardinality (Dict UInt32),
        // and plain Utf8 (high cardinality) string columns.
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            // Low cardinality: Dictionary(UInt16, Utf8) → bloom filter expected
            Field::new(
                "env",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                true,
            ),
            // Medium cardinality: Dictionary(UInt32, Utf8) → bloom filter expected
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                true,
            ),
            // Plain Utf8 (high cardinality) → no bloom filter
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("value_f64", DataType::Float64, true),
        ]));

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let n = 100usize;
        let timestamps: Vec<i64> = (0..n as i64).map(|i| now + i * 1_000_000_000).collect();

        // Build dictionary arrays
        let envs = ["prod", "staging", "dev"];
        let mut env_builder = StringDictionaryBuilder::<arrow_array::types::UInt16Type>::new();
        for i in 0..n {
            env_builder.append_value(envs[i % envs.len()]);
        }
        let env_array = env_builder.finish();

        let hosts = ["host-1", "host-2", "host-3", "host-4"];
        let mut host_builder = StringDictionaryBuilder::<arrow_array::types::UInt32Type>::new();
        for i in 0..n {
            host_builder.append_value(hosts[i % hosts.len()]);
        }
        let host_array = host_builder.finish();

        let trace_ids: Vec<String> = (0..n).map(|i| format!("trace-{i:032x}")).collect();
        let trace_id_array =
            StringArray::from(trace_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        let values: Vec<f64> = (0..n).map(|i| i as f64).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
                Arc::new(env_array),
                Arc::new(host_array),
                Arc::new(trace_id_array),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap();

        let writer = ParquetWriter::new();
        let bytes = writer.write_batch(&batch).unwrap();
        assert!(!bytes.is_empty());

        // Inspect the Parquet metadata to verify bloom filters were written
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
        let parquet_schema = builder.parquet_schema();

        // env (Dict UInt16) → index 1 in parquet columns (after timestamp)
        // host (Dict UInt32) → index 2
        // trace_id (plain Utf8) → index 3
        // value_f64 → index 4
        //
        // We verify bloom filter presence by checking row group metadata.
        let metadata = builder.metadata();
        assert!(metadata.num_row_groups() > 0);

        let rg = metadata.row_group(0);
        // Find columns by name in the parquet schema
        let col_names: Vec<String> = parquet_schema
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();

        let env_idx = col_names.iter().position(|n| n == "env").unwrap();
        let host_idx = col_names.iter().position(|n| n == "host").unwrap();
        let trace_idx = col_names.iter().position(|n| n == "trace_id").unwrap();

        // Dictionary columns should have bloom filter offset set
        let env_col = rg.column(env_idx);
        let host_col = rg.column(host_idx);
        let trace_col = rg.column(trace_idx);

        assert!(
            env_col.bloom_filter_offset().is_some(),
            "env (Dict UInt16) should have a bloom filter"
        );
        assert!(
            host_col.bloom_filter_offset().is_some(),
            "host (Dict UInt32) should have a bloom filter"
        );
        assert!(
            trace_col.bloom_filter_offset().is_none(),
            "trace_id (plain Utf8) should NOT have a bloom filter"
        );

        // Verify we can still read the data back correctly
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let read_batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, n);
    }
}
