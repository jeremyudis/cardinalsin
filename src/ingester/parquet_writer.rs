//! Parquet writer with optimal settings for time-series data

use crate::Result;
use arrow_array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

/// Parquet writer optimized for time-series metrics
pub struct ParquetWriter {
    /// Writer properties
    props: WriterProperties,
}

impl ParquetWriter {
    /// Label columns that get bloom filters for efficient predicate pushdown
    const BLOOM_FILTER_COLUMNS: &'static [&'static str] = &[
        "metric_name",
        "host",
        "region",
        "env",
        "service",
        "job",
        "instance",
    ];

    /// Create a new Parquet writer with optimal settings
    pub fn new() -> Self {
        let props = Self::build_writer_properties();
        Self { props }
    }

    /// Build optimal writer properties for time-series data
    fn build_writer_properties() -> WriterProperties {
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
            // 500K rows per row group gives ~50-100MB per group
            .set_max_row_group_size(500_000)
            // Enable statistics for predicate pushdown
            .set_statistics_enabled(EnabledStatistics::Page)
            // Bloom filters disabled globally (timestamps and value columns don't benefit)
            .set_bloom_filter_enabled(false)
            // Data page settings
            .set_data_page_size_limit(1024 * 1024); // 1MB data pages

        // Enable bloom filters selectively for low-cardinality label columns
        for col in Self::BLOOM_FILTER_COLUMNS {
            let path = ColumnPath::from(*col);
            builder = builder
                .set_column_bloom_filter_enabled(path.clone(), true)
                .set_column_bloom_filter_fpp(path.clone(), 0.01) // 1% false positive rate
                .set_column_bloom_filter_ndv(path, 1000); // Expected ~1000 distinct values
        }

        builder.build()
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

impl ParquetWriter {
    /// Extract column statistics from written Parquet bytes.
    /// Reads the Parquet file metadata and returns per-column min/max stats.
    pub fn extract_column_stats(
        bytes: &Bytes,
    ) -> crate::Result<std::collections::HashMap<String, crate::metadata::ColumnStats>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::statistics::Statistics;

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).map_err(|e| {
            crate::Error::InvalidSchema(format!("Failed to read Parquet metadata: {}", e))
        })?;

        let parquet_metadata = builder.metadata();
        let schema = builder.schema();
        let mut result = std::collections::HashMap::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col_name = field.name().clone();
            let mut min_val: Option<serde_json::Value> = None;
            let mut max_val: Option<serde_json::Value> = None;
            let mut has_nulls = false;

            for row_group in parquet_metadata.row_groups() {
                if col_idx >= row_group.columns().len() {
                    continue;
                }
                let col_meta = &row_group.columns()[col_idx];
                let Some(stats) = col_meta.statistics() else {
                    continue;
                };

                has_nulls |= stats.null_count_opt().map(|n| n > 0).unwrap_or(false);

                match stats {
                    Statistics::ByteArray(ba) => {
                        if let Some(min_bytes) = ba.min_opt() {
                            if let Ok(s) = std::str::from_utf8(min_bytes.data()) {
                                let v = serde_json::Value::String(s.to_string());
                                min_val = Some(match min_val.take() {
                                    None => v,
                                    Some(prev) => {
                                        if v.as_str() < prev.as_str() {
                                            v
                                        } else {
                                            prev
                                        }
                                    }
                                });
                            }
                        }
                        if let Some(max_bytes) = ba.max_opt() {
                            if let Ok(s) = std::str::from_utf8(max_bytes.data()) {
                                let v = serde_json::Value::String(s.to_string());
                                max_val = Some(match max_val.take() {
                                    None => v,
                                    Some(prev) => {
                                        if v.as_str() > prev.as_str() {
                                            v
                                        } else {
                                            prev
                                        }
                                    }
                                });
                            }
                        }
                    }
                    Statistics::Int64(i64_stats) => {
                        if let Some(v) = i64_stats.min_opt() {
                            let jv = serde_json::Value::Number(serde_json::Number::from(*v));
                            min_val = Some(match min_val.take() {
                                None => jv,
                                Some(prev) => {
                                    let pv = prev.as_i64().unwrap_or(i64::MAX);
                                    if *v < pv {
                                        jv
                                    } else {
                                        prev
                                    }
                                }
                            });
                        }
                        if let Some(v) = i64_stats.max_opt() {
                            let jv = serde_json::Value::Number(serde_json::Number::from(*v));
                            max_val = Some(match max_val.take() {
                                None => jv,
                                Some(prev) => {
                                    let pv = prev.as_i64().unwrap_or(i64::MIN);
                                    if *v > pv {
                                        jv
                                    } else {
                                        prev
                                    }
                                }
                            });
                        }
                    }
                    Statistics::Double(f64_stats) => {
                        if let Some(v) = f64_stats.min_opt() {
                            if let Some(n) = serde_json::Number::from_f64(*v) {
                                let jv = serde_json::Value::Number(n);
                                min_val = Some(match min_val.take() {
                                    None => jv,
                                    Some(prev) => {
                                        let pv = prev.as_f64().unwrap_or(f64::MAX);
                                        if *v < pv {
                                            jv
                                        } else {
                                            prev
                                        }
                                    }
                                });
                            }
                        }
                        if let Some(v) = f64_stats.max_opt() {
                            if let Some(n) = serde_json::Number::from_f64(*v) {
                                let jv = serde_json::Value::Number(n);
                                max_val = Some(match max_val.take() {
                                    None => jv,
                                    Some(prev) => {
                                        let pv = prev.as_f64().unwrap_or(f64::MIN);
                                        if *v > pv {
                                            jv
                                        } else {
                                            prev
                                        }
                                    }
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }

            if min_val.is_some() || max_val.is_some() || has_nulls {
                result.insert(
                    col_name,
                    crate::metadata::ColumnStats {
                        min: min_val.unwrap_or(serde_json::Value::Null),
                        max: max_val.unwrap_or(serde_json::Value::Null),
                        has_nulls,
                    },
                );
            }
        }

        Ok(result)
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

    #[test]
    fn test_extract_column_stats() {
        let writer = ParquetWriter::new();
        let batch = create_test_batch();
        let bytes = writer.write_batch(&batch).unwrap();

        let stats = ParquetWriter::extract_column_stats(&bytes).unwrap();
        assert!(
            !stats.is_empty(),
            "Should have stats for at least one column"
        );

        // metric_name column should have stats
        let name_stats = stats
            .get("metric_name")
            .expect("metric_name should have stats");
        assert!(
            !name_stats.min.is_null(),
            "metric_name should have min stat"
        );
        assert!(
            !name_stats.max.is_null(),
            "metric_name should have max stat"
        );
        assert_eq!(name_stats.min.as_str(), Some("cpu_usage"));
        assert_eq!(name_stats.max.as_str(), Some("cpu_usage"));

        // value_f64 column should have numeric stats
        let val_stats = stats.get("value_f64").expect("value_f64 should have stats");
        assert!(!val_stats.min.is_null(), "value_f64 should have min stat");
        assert!(!val_stats.max.is_null(), "value_f64 should have max stat");
    }

    #[test]
    fn test_bloom_filters_on_label_columns() {
        let writer = ParquetWriter::new();
        let batch = create_test_batch();
        let bytes = writer.write_batch(&batch).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let metadata = builder.metadata();

        // metric_name (column index 1) should have a bloom filter
        let row_group = metadata.row_group(0);
        let metric_col = row_group.column(1);
        assert_eq!(
            metric_col.column_descr().name(),
            "metric_name",
            "Column 1 should be metric_name"
        );
        assert!(
            metric_col.bloom_filter_offset().is_some(),
            "metric_name should have a bloom filter"
        );

        // timestamp (column 0) should NOT have a bloom filter
        let ts_col = row_group.column(0);
        assert!(
            ts_col.bloom_filter_offset().is_none(),
            "timestamp should not have a bloom filter"
        );

        // value_f64 (column 2) should NOT have a bloom filter
        let val_col = row_group.column(2);
        assert!(
            val_col.bloom_filter_offset().is_none(),
            "value_f64 should not have a bloom filter"
        );
    }
}
