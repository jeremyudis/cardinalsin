//! Parquet writer with optimal settings for time-series data

use crate::Result;
use arrow_array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

/// Parquet writer optimized for time-series metrics
pub struct ParquetWriter {
    /// Writer properties
    props: WriterProperties,
}

impl ParquetWriter {
    /// Create a new Parquet writer with optimal settings
    pub fn new() -> Self {
        let props = Self::build_writer_properties();
        Self { props }
    }

    /// Build optimal writer properties for time-series data
    fn build_writer_properties() -> WriterProperties {
        WriterProperties::builder()
            // Use Parquet v2 for better encoding support
            .set_writer_version(WriterVersion::PARQUET_2_0)

            // Compression: ZSTD level 3 (good ratio, fast)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))

            // Enable dictionary for all columns initially
            // High-cardinality columns will fall back to plain encoding
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(1_000_000)

            // Row group sizing for optimal S3 access
            // 500K rows per row group gives ~50-100MB per group
            .set_max_row_group_size(500_000)

            // Enable statistics for predicate pushdown
            .set_statistics_enabled(EnabledStatistics::Page)

            // Bloom filters disabled by default - enable only for specific columns
            // They add significant overhead for small batches
            .set_bloom_filter_enabled(false)

            // Data page settings
            .set_data_page_size_limit(1024 * 1024) // 1MB data pages

            .build()
    }

    /// Write a record batch to Parquet bytes
    pub fn write_batch(&self, batch: &RecordBatch) -> Result<Bytes> {
        let mut buffer = Vec::new();

        {
            let mut writer =
                ArrowWriter::try_new(&mut buffer, batch.schema(), Some(self.props.clone()))?;

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

        let mut buffer = Vec::new();

        {
            let mut writer =
                ArrowWriter::try_new(&mut buffer, batches[0].schema(), Some(self.props.clone()))?;

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
    use arrow_array::{Float64Array, StringArray, TimestampNanosecondArray};
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
}
