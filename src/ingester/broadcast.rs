//! Broadcast channel for streaming query subscribers

use arrow_array::RecordBatch;
use tokio::sync::broadcast;

/// Broadcast channel for distributing new data to streaming query subscribers
#[derive(Debug)]
pub struct BroadcastChannel {
    sender: broadcast::Sender<RecordBatch>,
}

impl BroadcastChannel {
    /// Create a new broadcast channel with the given capacity
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Send a batch to all subscribers
    pub fn send(
        &self,
        batch: RecordBatch,
    ) -> Result<usize, broadcast::error::SendError<RecordBatch>> {
        self.sender.send(batch)
    }

    /// Subscribe to receive new batches
    pub fn subscribe(&self) -> broadcast::Receiver<RecordBatch> {
        self.sender.subscribe()
    }

    /// Get the current number of receivers
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Clone for BroadcastChannel {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_broadcast() {
        let channel = BroadcastChannel::new(16);

        let mut rx1 = channel.subscribe();
        let mut rx2 = channel.subscribe();

        let batch = create_test_batch();
        let count = channel.send(batch.clone()).unwrap();
        assert_eq!(count, 2);

        let received1 = rx1.recv().await.unwrap();
        let received2 = rx2.recv().await.unwrap();

        assert_eq!(received1.num_rows(), 3);
        assert_eq!(received2.num_rows(), 3);
    }

    #[test]
    fn test_no_subscribers() {
        let channel = BroadcastChannel::new(16);
        let batch = create_test_batch();

        // Should not panic when no subscribers
        let result = channel.send(batch);
        assert!(result.is_err()); // No receivers
    }
}
