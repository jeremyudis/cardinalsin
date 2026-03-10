//! Chunk merging logic

use crate::{Error, Result};

use arrow::compute::{concat_batches, sort_to_indices, take};
use arrow_array::{new_null_array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Merges multiple Parquet chunks into one
pub struct ChunkMerger {
    object_store: Arc<dyn ObjectStore>,
}

impl ChunkMerger {
    /// Create a new chunk merger
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }

    /// Compute the union schema across all batches, preserving field order
    /// by insertion order (first-seen wins position).
    fn union_schema(batches: &[RecordBatch]) -> Result<SchemaRef> {
        let mut fields: BTreeMap<String, arrow_schema::FieldRef> = BTreeMap::new();
        let mut order: Vec<String> = Vec::new();

        for batch in batches {
            for field in batch.schema().fields() {
                if !fields.contains_key(field.name()) {
                    // Mark nullable since other batches may not have this column
                    let nullable_field = if field.is_nullable() {
                        field.clone()
                    } else {
                        // Columns that exist in some batches but not others must be nullable
                        // in the union schema. Keep non-nullable only if ALL batches have it.
                        field.clone()
                    };
                    fields.insert(field.name().clone(), nullable_field);
                    order.push(field.name().clone());
                }
            }
        }

        // Second pass: mark fields nullable if any batch is missing them
        let all_have: std::collections::HashSet<String> = {
            let mut common: Option<std::collections::HashSet<String>> = None;
            for batch in batches {
                let names: std::collections::HashSet<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                common = Some(match common {
                    None => names,
                    Some(prev) => prev.intersection(&names).cloned().collect(),
                });
            }
            common.unwrap_or_default()
        };

        let union_fields: Vec<arrow_schema::FieldRef> = order
            .iter()
            .map(|name| {
                let field = &fields[name];
                if all_have.contains(name) {
                    field.clone()
                } else {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                }
            })
            .collect();

        Ok(Arc::new(Schema::new(union_fields)))
    }

    /// Project a batch to a target schema, filling missing columns with nulls.
    fn project_to_schema(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let columns: Vec<Arc<dyn arrow_array::Array>> = target
            .fields()
            .iter()
            .map(|field| match batch.column_by_name(field.name()) {
                Some(col) => col.clone(),
                None => new_null_array(field.data_type(), num_rows),
            })
            .collect();

        RecordBatch::try_new(target.clone(), columns)
            .map_err(|e| Error::Internal(format!("Failed to project batch to union schema: {e}")))
    }

    /// Merge multiple chunks into a single RecordBatch
    pub async fn merge(&self, paths: &[String]) -> Result<RecordBatch> {
        let mut batches = Vec::new();

        for path in paths {
            let chunk_batches = self.read_chunk(path).await?;
            batches.extend(chunk_batches);
        }

        if batches.is_empty() {
            return Err(Error::InvalidSchema("No data to merge".into()));
        }

        // Compute union schema and project all batches to it
        let schema = Self::union_schema(&batches)?;
        let projected: Vec<RecordBatch> = batches
            .iter()
            .map(|b| Self::project_to_schema(b, &schema))
            .collect::<Result<_>>()?;

        let merged = concat_batches(&schema, &projected)?;

        Ok(merged)
    }

    /// Read a Parquet chunk from object storage
    async fn read_chunk(&self, path: &str) -> Result<Vec<RecordBatch>> {
        let data = self.object_store.get(&path.into()).await?.bytes().await?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(data)?.build()?;

        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Internal(format!("Failed to read Parquet row group: {}", e)))?;

        Ok(batches)
    }

    /// Sort a batch by timestamp and metric name
    pub fn sort_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // Get timestamp column for sorting
        let timestamp_col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| Error::InvalidSchema("Missing timestamp column".into()))?;

        // Get sort indices
        let indices = sort_to_indices(timestamp_col, None, None)?;

        // Apply sort to all columns
        let sorted_columns: Vec<Arc<dyn arrow_array::Array>> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect::<std::result::Result<_, _>>()?;

        let sorted = RecordBatch::try_new(batch.schema(), sorted_columns)?;
        Ok(sorted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, TimestampNanosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use parquet::arrow::ArrowWriter;

    async fn create_test_chunk(store: &InMemory, path: &str, timestamps: Vec<i64>) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("value_f64", DataType::Float64, true),
        ]));

        let values: Vec<f64> = timestamps.iter().map(|t| *t as f64 * 0.001).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
                Arc::new(Float64Array::from(values)),
            ],
        )?;

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
        }

        store.put(&path.into(), Bytes::from(buffer).into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_chunks() {
        let store = Arc::new(InMemory::new());

        // Create two chunks
        create_test_chunk(&store, "chunk1.parquet", vec![1000, 2000, 3000])
            .await
            .unwrap();
        create_test_chunk(&store, "chunk2.parquet", vec![4000, 5000, 6000])
            .await
            .unwrap();

        let merger = ChunkMerger::new(store);
        let merged = merger
            .merge(&["chunk1.parquet".to_string(), "chunk2.parquet".to_string()])
            .await
            .unwrap();

        assert_eq!(merged.num_rows(), 6);
    }

    /// Create a test chunk with a custom schema (simulates different ingestion paths)
    async fn create_chunk_with_schema(
        store: &InMemory,
        path: &str,
        schema: Arc<Schema>,
        columns: Vec<Arc<dyn arrow_array::Array>>,
    ) -> Result<()> {
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
        }
        store.put(&path.into(), Bytes::from(buffer).into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_heterogeneous_schemas() {
        use arrow_array::{Int64Array, StringArray};

        let store = Arc::new(InMemory::new());

        // Simulate OTel path: timestamp + value_f64 only
        let otel_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("value_f64", DataType::Float64, true),
        ]));
        create_chunk_with_schema(
            &store,
            "otel.parquet",
            otel_schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1000, 2000]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .await
        .unwrap();

        // Simulate Prometheus path: timestamp + value_f64 + value_i64 + host
        let prom_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("value_f64", DataType::Float64, true),
            Field::new("value_i64", DataType::Int64, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        create_chunk_with_schema(
            &store,
            "prom.parquet",
            prom_schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![3000, 4000]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![3.0, 4.0])),
                Arc::new(Int64Array::from(vec![30, 40])),
                Arc::new(StringArray::from(vec!["host-a", "host-b"])),
            ],
        )
        .await
        .unwrap();

        let merger = ChunkMerger::new(store);
        let merged = merger
            .merge(&["otel.parquet".to_string(), "prom.parquet".to_string()])
            .await
            .unwrap();

        // Should have all 4 rows
        assert_eq!(merged.num_rows(), 4);

        // Union schema should have all 4 columns
        assert_eq!(merged.num_columns(), 4);
        assert!(merged.column_by_name("timestamp").is_some());
        assert!(merged.column_by_name("value_f64").is_some());
        assert!(merged.column_by_name("value_i64").is_some());
        assert!(merged.column_by_name("host").is_some());

        // OTel rows should have nulls for value_i64 and host
        let host_col = merged
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(host_col.is_null(0)); // OTel row
        assert!(host_col.is_null(1)); // OTel row
        assert_eq!(host_col.value(2), "host-a"); // Prom row
        assert_eq!(host_col.value(3), "host-b"); // Prom row
    }

    #[tokio::test]
    async fn test_merge_heterogeneous_then_sort() {
        use arrow_array::Int64Array;

        let store = Arc::new(InMemory::new());

        // Chunk A: later timestamps, fewer columns
        let schema_a = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("value_f64", DataType::Float64, true),
        ]));
        create_chunk_with_schema(
            &store,
            "a.parquet",
            schema_a,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![5000, 3000]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![5.0, 3.0])),
            ],
        )
        .await
        .unwrap();

        // Chunk B: earlier timestamps, extra column
        let schema_b = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("value_f64", DataType::Float64, true),
            Field::new("value_i64", DataType::Int64, true),
        ]));
        create_chunk_with_schema(
            &store,
            "b.parquet",
            schema_b,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1000, 4000]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![1.0, 4.0])),
                Arc::new(Int64Array::from(vec![10, 40])),
            ],
        )
        .await
        .unwrap();

        let merger = ChunkMerger::new(store);
        let merged = merger
            .merge(&["a.parquet".to_string(), "b.parquet".to_string()])
            .await
            .unwrap();

        // Sort should work on the union-schema batch
        let sorted = merger.sort_batch(&merged).unwrap();

        use arrow_array::cast::AsArray;
        use arrow_array::types::TimestampNanosecondType;
        let ts = sorted
            .column_by_name("timestamp")
            .unwrap()
            .as_primitive::<TimestampNanosecondType>();
        assert_eq!(ts.value(0), 1000);
        assert_eq!(ts.value(1), 3000);
        assert_eq!(ts.value(2), 4000);
        assert_eq!(ts.value(3), 5000);
    }

    #[tokio::test]
    async fn test_sort_batch() {
        let store = Arc::new(InMemory::new());

        // Create chunk with unsorted timestamps
        create_test_chunk(&store, "unsorted.parquet", vec![3000, 1000, 2000])
            .await
            .unwrap();

        let merger = ChunkMerger::new(store.clone());
        let batches = merger.read_chunk("unsorted.parquet").await.unwrap();
        let sorted = merger.sort_batch(&batches[0]).unwrap();

        // Verify sorted order
        use arrow_array::cast::AsArray;
        use arrow_array::types::TimestampNanosecondType;

        let ts_col = sorted.column_by_name("timestamp").unwrap();
        let ts_array = ts_col.as_primitive::<TimestampNanosecondType>();

        assert_eq!(ts_array.value(0), 1000);
        assert_eq!(ts_array.value(1), 2000);
        assert_eq!(ts_array.value(2), 3000);
    }
}
