//! Chunk merging logic

use crate::{Error, Result};

use arrow_array::RecordBatch;
use arrow::compute::{concat_batches, sort_to_indices, take};
use bytes::Bytes;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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

        // Concatenate all batches
        let schema = batches[0].schema();
        let merged = concat_batches(&schema, &batches)?;

        Ok(merged)
    }

    /// Read a Parquet chunk from object storage
    async fn read_chunk(&self, path: &str) -> Result<Vec<RecordBatch>> {
        let data = self.object_store
            .get(&path.into())
            .await?
            .bytes()
            .await?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(data)?
            .build()?;

        let batches: Vec<RecordBatch> = reader
            .filter_map(|r| r.ok())
            .collect();

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
            .map(|col| take(col.as_ref(), &indices, None).map(Arc::from))
            .collect::<std::result::Result<_, _>>()?;

        let sorted = RecordBatch::try_new(batch.schema(), sorted_columns)?;
        Ok(sorted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use arrow_array::{Int64Array, Float64Array, TimestampNanosecondArray};
    use arrow_schema::{Schema, Field, DataType, TimeUnit};
    use parquet::arrow::ArrowWriter;

    async fn create_test_chunk(store: &InMemory, path: &str, timestamps: Vec<i64>) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
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
        create_test_chunk(&store, "chunk1.parquet", vec![1000, 2000, 3000]).await.unwrap();
        create_test_chunk(&store, "chunk2.parquet", vec![4000, 5000, 6000]).await.unwrap();

        let merger = ChunkMerger::new(store);
        let merged = merger.merge(&[
            "chunk1.parquet".to_string(),
            "chunk2.parquet".to_string(),
        ]).await.unwrap();

        assert_eq!(merged.num_rows(), 6);
    }

    #[tokio::test]
    async fn test_sort_batch() {
        let store = Arc::new(InMemory::new());

        // Create chunk with unsorted timestamps
        create_test_chunk(&store, "unsorted.parquet", vec![3000, 1000, 2000]).await.unwrap();

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
