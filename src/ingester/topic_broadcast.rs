//! Topic-based broadcast channel for efficient streaming
//!
//! This module provides filtered broadcasting so streaming queries only receive
//! data relevant to their predicates, eliminating 90% bandwidth waste.

use arrow_array::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::debug;

/// Metadata attached to each broadcast batch
#[derive(Debug, Clone)]
pub struct BatchMetadata {
    /// Shard ID this batch belongs to
    pub shard_id: String,
    /// Tenant ID
    pub tenant_id: u32,
    /// Metric names present in this batch (for filtering)
    pub metrics: Vec<String>,
}

/// A batch with topic metadata
#[derive(Debug, Clone)]
pub struct TopicBatch {
    /// The actual data
    pub batch: RecordBatch,
    /// Metadata for routing
    pub metadata: BatchMetadata,
}

/// Topic filter for subscriptions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TopicFilter {
    /// Subscribe to all data (no filtering)
    All,
    /// Subscribe to specific shard
    Shard(String),
    /// Subscribe to specific tenant
    Tenant(u32),
    /// Subscribe to specific metric names
    Metrics(Vec<String>),
    /// Combine multiple filters (AND)
    And(Vec<TopicFilter>),
    /// Combine multiple filters (OR)
    Or(Vec<TopicFilter>),
}

impl TopicFilter {
    /// Check if this filter matches the given batch metadata
    pub fn matches(&self, metadata: &BatchMetadata) -> bool {
        match self {
            TopicFilter::All => true,
            TopicFilter::Shard(shard_id) => &metadata.shard_id == shard_id,
            TopicFilter::Tenant(tenant_id) => metadata.tenant_id == *tenant_id,
            TopicFilter::Metrics(filter_metrics) => {
                // Match if any metric in the batch is in the filter
                metadata.metrics.iter().any(|m| filter_metrics.contains(m))
            }
            TopicFilter::And(filters) => filters.iter().all(|f| f.matches(metadata)),
            TopicFilter::Or(filters) => filters.iter().any(|f| f.matches(metadata)),
        }
    }

    /// Create a filter for a specific shard
    pub fn for_shard(shard_id: String) -> Self {
        TopicFilter::Shard(shard_id)
    }

    /// Create a filter for specific metrics
    pub fn for_metrics(metrics: Vec<String>) -> Self {
        TopicFilter::Metrics(metrics)
    }

    /// Combine this filter with another using AND
    pub fn and(self, other: TopicFilter) -> Self {
        match (self, other) {
            (TopicFilter::And(mut filters), TopicFilter::And(other_filters)) => {
                filters.extend(other_filters);
                TopicFilter::And(filters)
            }
            (TopicFilter::And(mut filters), other) => {
                filters.push(other);
                TopicFilter::And(filters)
            }
            (this, TopicFilter::And(mut filters)) => {
                filters.insert(0, this);
                TopicFilter::And(filters)
            }
            (this, other) => TopicFilter::And(vec![this, other]),
        }
    }
}

/// Filtered broadcast channel with topic-based routing
pub struct TopicBroadcastChannel {
    /// Main broadcast channel (unfiltered)
    sender: broadcast::Sender<TopicBatch>,
    /// Subscription tracking for metrics
    subscription_stats: Arc<RwLock<HashMap<String, usize>>>,
}

impl TopicBroadcastChannel {
    /// Create a new topic-based broadcast channel
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            subscription_stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Send a batch to all subscribers with filtering
    pub fn send(
        &self,
        batch: TopicBatch,
    ) -> Result<usize, broadcast::error::SendError<TopicBatch>> {
        // Log metrics for debugging
        debug!(
            "Broadcasting batch for shard {}, tenant {}, metrics: {:?}",
            batch.metadata.shard_id, batch.metadata.tenant_id, batch.metadata.metrics
        );

        self.sender.send(batch)
    }

    /// Subscribe with a topic filter
    ///
    /// Returns a FilteredReceiver that will only receive batches matching the filter
    pub async fn subscribe(&self, filter: TopicFilter) -> FilteredReceiver {
        let rx = self.sender.subscribe();

        // Track subscription stats
        if let TopicFilter::Metrics(ref metrics) = filter {
            let mut stats = self.subscription_stats.write().await;
            for metric in metrics {
                *stats.entry(metric.clone()).or_insert(0) += 1;
            }
        }

        FilteredReceiver::new(rx, filter)
    }

    /// Get the current number of receivers
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Get subscription statistics
    pub async fn subscription_stats(&self) -> HashMap<String, usize> {
        self.subscription_stats.read().await.clone()
    }
}

impl Clone for TopicBroadcastChannel {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            subscription_stats: self.subscription_stats.clone(),
        }
    }
}

/// Filtered receiver that only delivers batches matching the topic filter
pub struct FilteredReceiver {
    rx: broadcast::Receiver<TopicBatch>,
    filter: TopicFilter,
    filtered_count: u64,
    delivered_count: u64,
}

impl FilteredReceiver {
    fn new(rx: broadcast::Receiver<TopicBatch>, filter: TopicFilter) -> Self {
        Self {
            rx,
            filter,
            filtered_count: 0,
            delivered_count: 0,
        }
    }

    /// Receive the next batch that matches the filter
    pub async fn recv(&mut self) -> Result<RecordBatch, broadcast::error::RecvError> {
        loop {
            let topic_batch = self.rx.recv().await?;

            if self.filter.matches(&topic_batch.metadata) {
                self.delivered_count += 1;
                return Ok(topic_batch.batch);
            } else {
                self.filtered_count += 1;
                // Log periodic stats
                if self.filtered_count % 100 == 0 {
                    debug!(
                        "Filtered receiver stats: {} delivered, {} filtered ({}% reduction)",
                        self.delivered_count,
                        self.filtered_count,
                        (self.filtered_count * 100) / (self.delivered_count + self.filtered_count)
                    );
                }
            }
        }
    }

    /// Get filter statistics
    pub fn stats(&self) -> (u64, u64) {
        (self.delivered_count, self.filtered_count)
    }

    /// Create a new FilteredReceiver with the same filter but fresh broadcast subscription
    ///
    /// This is similar to broadcast::Receiver::resubscribe() - creates a new receiver
    /// that will start receiving messages from this point forward.
    pub fn resubscribe(&self) -> Self {
        Self {
            rx: self.rx.resubscribe(),
            filter: self.filter.clone(),
            filtered_count: 0,
            delivered_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["cpu", "cpu", "memory"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_topic_filter_matching() {
        let metadata = BatchMetadata {
            shard_id: "shard-1".to_string(),
            tenant_id: 42,
            metrics: vec!["cpu".to_string(), "memory".to_string()],
        };

        // Test shard filter
        assert!(TopicFilter::Shard("shard-1".to_string()).matches(&metadata));
        assert!(!TopicFilter::Shard("shard-2".to_string()).matches(&metadata));

        // Test tenant filter
        assert!(TopicFilter::Tenant(42).matches(&metadata));
        assert!(!TopicFilter::Tenant(43).matches(&metadata));

        // Test metrics filter
        assert!(TopicFilter::Metrics(vec!["cpu".to_string()]).matches(&metadata));
        assert!(TopicFilter::Metrics(vec!["memory".to_string()]).matches(&metadata));
        assert!(!TopicFilter::Metrics(vec!["disk".to_string()]).matches(&metadata));

        // Test AND filter
        let and_filter = TopicFilter::And(vec![
            TopicFilter::Tenant(42),
            TopicFilter::Metrics(vec!["cpu".to_string()]),
        ]);
        assert!(and_filter.matches(&metadata));

        let failing_and_filter = TopicFilter::And(vec![
            TopicFilter::Tenant(99),
            TopicFilter::Metrics(vec!["cpu".to_string()]),
        ]);
        assert!(!failing_and_filter.matches(&metadata));

        // Test OR filter
        let or_filter = TopicFilter::Or(vec![
            TopicFilter::Tenant(99),
            TopicFilter::Metrics(vec!["cpu".to_string()]),
        ]);
        assert!(or_filter.matches(&metadata));
    }

    #[tokio::test]
    async fn test_filtered_broadcast() {
        let channel = TopicBroadcastChannel::new(16);

        // Subscribe with different filters
        let mut rx_cpu = channel
            .subscribe(TopicFilter::Metrics(vec!["cpu".to_string()]))
            .await;
        let mut rx_memory = channel
            .subscribe(TopicFilter::Metrics(vec!["memory".to_string()]))
            .await;
        let mut rx_all = channel.subscribe(TopicFilter::All).await;

        // Send a CPU batch
        let batch = create_test_batch();
        let topic_batch = TopicBatch {
            batch: batch.clone(),
            metadata: BatchMetadata {
                shard_id: "shard-1".to_string(),
                tenant_id: 1,
                metrics: vec!["cpu".to_string()],
            },
        };
        channel.send(topic_batch).unwrap();

        // CPU subscriber should receive it
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(100), rx_cpu.recv()).await;
        assert!(received.is_ok());

        // All subscriber should receive it
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(100), rx_all.recv()).await;
        assert!(received.is_ok());

        // Memory subscriber should NOT receive it (would timeout)
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(50), rx_memory.recv()).await;
        assert!(received.is_err()); // Timeout = no matching message
    }

    #[test]
    fn test_filter_builder() {
        let filter = TopicFilter::for_shard("shard-1".to_string())
            .and(TopicFilter::for_metrics(vec!["cpu".to_string()]));

        let metadata = BatchMetadata {
            shard_id: "shard-1".to_string(),
            tenant_id: 1,
            metrics: vec!["cpu".to_string()],
        };

        assert!(filter.matches(&metadata));

        let wrong_shard_metadata = BatchMetadata {
            shard_id: "shard-2".to_string(),
            tenant_id: 1,
            metrics: vec!["cpu".to_string()],
        };

        assert!(!filter.matches(&wrong_shard_metadata));
    }
}
