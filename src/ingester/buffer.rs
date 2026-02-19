//! Write buffer for batching incoming metrics

use crate::Result;
use arrow_array::RecordBatch;

/// Write buffer that accumulates record batches before flushing
#[derive(Debug)]
pub struct WriteBuffer {
    /// Accumulated batches
    batches: Vec<RecordBatch>,
    /// Total row count
    row_count: usize,
    /// Total size in bytes (approximate)
    size_bytes: usize,
}

impl WriteBuffer {
    /// Create a new empty write buffer
    pub fn new() -> Self {
        Self {
            batches: Vec::new(),
            row_count: 0,
            size_bytes: 0,
        }
    }

    /// Append a record batch to the buffer
    pub fn append(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = batch.num_rows();
        let size = batch.get_array_memory_size();

        self.batches.push(batch);
        self.row_count += rows;
        self.size_bytes += size;

        Ok(())
    }

    /// Take all batches from the buffer, leaving it empty
    pub fn take(&mut self) -> Vec<RecordBatch> {
        self.row_count = 0;
        self.size_bytes = 0;
        std::mem::take(&mut self.batches)
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Get the total row count
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the total size in bytes
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Get the number of batches
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.batches.clear();
        self.row_count = 0;
        self.size_bytes = 0;
    }
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let timestamps: Vec<i64> = (0..rows as i64).collect();
        let values: Vec<f64> = (0..rows).map(|i| i as f64 * 0.1).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_buffer_append() {
        let mut buffer = WriteBuffer::new();
        assert!(buffer.is_empty());

        let batch = create_test_batch(100);
        buffer.append(batch).unwrap();

        assert!(!buffer.is_empty());
        assert_eq!(buffer.row_count(), 100);
        assert_eq!(buffer.batch_count(), 1);
    }

    #[test]
    fn test_buffer_take() {
        let mut buffer = WriteBuffer::new();

        buffer.append(create_test_batch(100)).unwrap();
        buffer.append(create_test_batch(200)).unwrap();

        assert_eq!(buffer.row_count(), 300);
        assert_eq!(buffer.batch_count(), 2);

        let batches = buffer.take();
        assert_eq!(batches.len(), 2);
        assert!(buffer.is_empty());
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn test_buffer_clear() {
        let mut buffer = WriteBuffer::new();
        buffer.append(create_test_batch(100)).unwrap();

        buffer.clear();
        assert!(buffer.is_empty());
    }
}
