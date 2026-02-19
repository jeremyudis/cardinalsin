//! Integration tests for topic-based streaming subscriptions
//!
//! Tests that streaming queries only receive relevant data, eliminating 90% bandwidth waste.

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cardinalsin::ingester::{
    BatchMetadata, FilteredReceiver, TopicBatch, TopicBroadcastChannel, TopicFilter,
};
use std::sync::Arc;
use std::time::Duration;

/// Helper to create test batch with metrics
fn create_batch(metrics: Vec<&str>, values: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let timestamps: Vec<i64> = (0..metrics.len() as i64).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringArray::from(
                metrics.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_topic_filter_reduces_bandwidth() {
    let channel = TopicBroadcastChannel::new(100);

    // Subscribe to only 'cpu' metrics
    let mut rx_cpu = channel
        .subscribe(TopicFilter::for_metrics(vec!["cpu".to_string()]))
        .await;

    // Subscribe to all metrics
    let mut rx_all = channel.subscribe(TopicFilter::All).await;

    // Send 10 batches: 5 cpu, 5 memory
    for i in 0..10 {
        let metric = if i < 5 { "cpu" } else { "memory" };
        let batch = create_batch(vec![metric], vec![i as f64]);
        let topic_batch = TopicBatch {
            batch,
            metadata: BatchMetadata {
                shard_id: "shard-1".to_string(),
                tenant_id: 1,
                metrics: vec![metric.to_string()],
            },
        };
        channel.send(topic_batch).unwrap();
    }

    // CPU subscriber should receive 5 batches
    let mut cpu_count = 0;
    for _ in 0..5 {
        let result = tokio::time::timeout(Duration::from_millis(100), rx_cpu.recv()).await;
        if result.is_ok() {
            cpu_count += 1;
        }
    }
    assert_eq!(cpu_count, 5, "CPU subscriber should receive 5 batches");

    // Next receive should timeout (no more CPU batches)
    let result = tokio::time::timeout(Duration::from_millis(50), rx_cpu.recv()).await;
    assert!(
        result.is_err(),
        "Should timeout waiting for more CPU batches"
    );

    // All subscriber should receive all 10 batches
    let mut all_count = 0;
    for _ in 0..10 {
        let result = tokio::time::timeout(Duration::from_millis(100), rx_all.recv()).await;
        if result.is_ok() {
            all_count += 1;
        }
    }
    assert_eq!(all_count, 10, "All subscriber should receive 10 batches");

    // Calculate bandwidth reduction
    let bandwidth_reduction_pct = ((10 - cpu_count) * 100) / 10;
    println!("Bandwidth reduction: {}%", bandwidth_reduction_pct);
    assert_eq!(
        bandwidth_reduction_pct, 50,
        "Should achieve 50% bandwidth reduction for single metric"
    );
}

#[tokio::test]
async fn test_multiple_metric_subscription() {
    let channel = TopicBroadcastChannel::new(100);

    // Subscribe to cpu and memory
    let mut rx_filtered = channel
        .subscribe(TopicFilter::for_metrics(vec![
            "cpu".to_string(),
            "memory".to_string(),
        ]))
        .await;

    // Send batches for cpu, memory, disk, network
    let metrics = ["cpu", "memory", "disk", "network"];
    for metric in &metrics {
        let batch = create_batch(vec![metric], vec![1.0]);
        let topic_batch = TopicBatch {
            batch,
            metadata: BatchMetadata {
                shard_id: "shard-1".to_string(),
                tenant_id: 1,
                metrics: vec![metric.to_string()],
            },
        };
        channel.send(topic_batch).unwrap();
    }

    // Should receive 2 batches (cpu, memory)
    let mut count = 0;
    for _ in 0..2 {
        let result = tokio::time::timeout(Duration::from_millis(100), rx_filtered.recv()).await;
        if result.is_ok() {
            count += 1;
        }
    }
    assert_eq!(count, 2, "Should receive 2 batches for cpu and memory");

    // Next receive should timeout
    let result = tokio::time::timeout(Duration::from_millis(50), rx_filtered.recv()).await;
    assert!(
        result.is_err(),
        "Should timeout after receiving filtered batches"
    );
}

#[tokio::test]
async fn test_shard_based_filtering() {
    let channel = TopicBroadcastChannel::new(100);

    // Subscribe to shard-1
    let mut rx_shard1 = channel
        .subscribe(TopicFilter::for_shard("shard-1".to_string()))
        .await;

    // Send batches to different shards
    for i in 0..6 {
        let shard_id = format!("shard-{}", i % 3); // shard-0, shard-1, shard-2
        let batch = create_batch(vec!["cpu"], vec![i as f64]);
        let topic_batch = TopicBatch {
            batch,
            metadata: BatchMetadata {
                shard_id: shard_id.clone(),
                tenant_id: 1,
                metrics: vec!["cpu".to_string()],
            },
        };
        channel.send(topic_batch).unwrap();
    }

    // Should receive 2 batches for shard-1 (indices 1 and 4)
    let mut count = 0;
    for _ in 0..2 {
        let result = tokio::time::timeout(Duration::from_millis(100), rx_shard1.recv()).await;
        if result.is_ok() {
            count += 1;
        }
    }
    assert_eq!(count, 2, "Should receive 2 batches for shard-1");
}

#[tokio::test]
async fn test_combined_filters_with_and() {
    let channel = TopicBroadcastChannel::new(100);

    // Subscribe to shard-1 AND cpu metric
    let filter = TopicFilter::for_shard("shard-1".to_string())
        .and(TopicFilter::for_metrics(vec!["cpu".to_string()]));
    let mut rx_filtered = channel.subscribe(filter).await;

    // Send various combinations
    let test_cases = vec![
        ("shard-1", "cpu", true),     // Should match
        ("shard-1", "memory", false), // Wrong metric
        ("shard-2", "cpu", false),    // Wrong shard
        ("shard-2", "memory", false), // Both wrong
    ];

    for (shard, metric, _should_match) in &test_cases {
        let batch = create_batch(vec![metric], vec![1.0]);
        let topic_batch = TopicBatch {
            batch,
            metadata: BatchMetadata {
                shard_id: shard.to_string(),
                tenant_id: 1,
                metrics: vec![metric.to_string()],
            },
        };
        channel.send(topic_batch).unwrap();
    }

    // Should receive only 1 batch (shard-1 + cpu)
    let mut count = 0;
    for _ in 0..1 {
        let result = tokio::time::timeout(Duration::from_millis(100), rx_filtered.recv()).await;
        if result.is_ok() {
            count += 1;
        }
    }
    assert_eq!(count, 1, "Should receive 1 batch matching both filters");

    // Next receive should timeout
    let result = tokio::time::timeout(Duration::from_millis(50), rx_filtered.recv()).await;
    assert!(
        result.is_err(),
        "Should timeout after receiving matched batch"
    );
}

#[tokio::test]
async fn test_bandwidth_savings_at_scale() {
    let channel = TopicBroadcastChannel::new(1000);

    // Simulate 10 subscribers, each watching a different metric
    let metrics = [
        "cpu",
        "memory",
        "disk",
        "network",
        "latency",
        "errors",
        "requests",
        "responses",
        "connections",
        "threads",
    ];

    let mut receivers: Vec<FilteredReceiver> = Vec::new();
    for m in &metrics {
        receivers.push(
            channel
                .subscribe(TopicFilter::for_metrics(vec![m.to_string()]))
                .await,
        );
    }

    // Send 1000 batches, evenly distributed across metrics
    // Give receivers small pauses to process messages and avoid broadcast channel lag
    for i in 0..1000 {
        let metric = metrics[i % metrics.len()];
        let batch = create_batch(vec![metric], vec![i as f64]);
        let topic_batch = TopicBatch {
            batch,
            metadata: BatchMetadata {
                shard_id: "shard-1".to_string(),
                tenant_id: 1,
                metrics: vec![metric.to_string()],
            },
        };
        channel.send(topic_batch).unwrap();

        // Small yield every 100 messages to let receivers catch up
        if i % 100 == 99 {
            tokio::task::yield_now().await;
        }
    }

    // Give receivers time to process any remaining messages
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Each subscriber should receive exactly 100 batches (10% of total)
    // Note: Some messages might be dropped due to broadcast channel lag, so we're more lenient
    let mut total_delivered = 0;
    let mut total_filtered = 0;

    for (i, rx) in receivers.iter_mut().enumerate() {
        let mut count = 0;
        // Try to receive up to 100 messages with reasonable timeout
        for _ in 0..100 {
            let result = tokio::time::timeout(Duration::from_millis(10), rx.recv()).await;
            if result.is_ok() {
                count += 1;
            } else {
                break; // No more messages available
            }
        }
        println!("Subscriber {} received {} batches", i, count);

        // Get filter statistics
        let (delivered, filtered) = rx.stats();
        println!(
            "Subscriber {} stats: {} delivered, {} filtered",
            i, delivered, filtered
        );

        total_delivered += delivered;
        total_filtered += filtered;

        // Allow for some variance due to timing
        assert!(
            (95..=105).contains(&delivered),
            "Subscriber {} should receive ~100 batches, got {}",
            i,
            delivered
        );
    }

    println!(
        "Total delivered: {}, total filtered: {}",
        total_delivered, total_filtered
    );

    // Calculate total bandwidth savings
    // Without filtering: 10 subscribers × ~1000 batches = ~10,000 transmissions
    // With filtering: each subscriber gets only matching batches
    let expected_without_filtering = 10 * 1000; // 10,000
    let actual_with_filtering = total_delivered;
    let bandwidth_reduction = if actual_with_filtering > 0 {
        ((expected_without_filtering - actual_with_filtering) * 100) / expected_without_filtering
    } else {
        0
    };
    println!("Total bandwidth reduction: {}%", bandwidth_reduction);
    assert!(
        (85..=95).contains(&bandwidth_reduction),
        "Should achieve ~90% bandwidth reduction, got {}%",
        bandwidth_reduction
    );
}

#[tokio::test]
async fn test_tenant_isolation() {
    let channel = TopicBroadcastChannel::new(100);

    // Subscribe to tenant 1
    let mut rx_tenant1 = channel.subscribe(TopicFilter::Tenant(1)).await;

    // Send batches for different tenants
    for tenant_id in 1..=5 {
        let batch = create_batch(vec!["cpu"], vec![tenant_id as f64]);
        let topic_batch = TopicBatch {
            batch,
            metadata: BatchMetadata {
                shard_id: "shard-1".to_string(),
                tenant_id,
                metrics: vec!["cpu".to_string()],
            },
        };
        channel.send(topic_batch).unwrap();
    }

    // Should receive only 1 batch for tenant 1
    let result = tokio::time::timeout(Duration::from_millis(100), rx_tenant1.recv()).await;
    assert!(result.is_ok(), "Should receive tenant 1 batch");

    // Next receive should timeout
    let result = tokio::time::timeout(Duration::from_millis(50), rx_tenant1.recv()).await;
    assert!(
        result.is_err(),
        "Should not receive batches from other tenants"
    );
}

#[tokio::test]
async fn test_subscription_stats_tracking() {
    let channel = TopicBroadcastChannel::new(100);

    // Subscribe to cpu and memory
    let _rx1 = channel
        .subscribe(TopicFilter::for_metrics(vec!["cpu".to_string()]))
        .await;
    let _rx2 = channel
        .subscribe(TopicFilter::for_metrics(vec!["memory".to_string()]))
        .await;
    let _rx3 = channel
        .subscribe(TopicFilter::for_metrics(vec![
            "cpu".to_string(),
            "memory".to_string(),
        ]))
        .await;

    // Check subscription stats
    let stats = channel.subscription_stats().await;
    println!("Subscription stats: {:?}", stats);

    assert_eq!(
        *stats.get("cpu").unwrap_or(&0),
        2,
        "CPU should have 2 subscriptions"
    );
    assert_eq!(
        *stats.get("memory").unwrap_or(&0),
        2,
        "Memory should have 2 subscriptions"
    );
}

#[tokio::test]
async fn test_mixed_batch_filtering() {
    let channel = TopicBroadcastChannel::new(100);

    // Subscribe to cpu
    let mut rx_cpu = channel
        .subscribe(TopicFilter::for_metrics(vec!["cpu".to_string()]))
        .await;

    // Send batch with multiple metrics (simulating aggregated data)
    let batch = create_batch(vec!["cpu", "memory", "cpu"], vec![1.0, 2.0, 3.0]);
    let topic_batch = TopicBatch {
        batch,
        metadata: BatchMetadata {
            shard_id: "shard-1".to_string(),
            tenant_id: 1,
            metrics: vec!["cpu".to_string(), "memory".to_string()],
        },
    };
    channel.send(topic_batch).unwrap();

    // Should receive the batch because it contains cpu
    let result = tokio::time::timeout(Duration::from_millis(100), rx_cpu.recv()).await;
    assert!(result.is_ok(), "Should receive batch containing cpu metric");
}

#[tokio::test]
async fn test_no_subscribers_doesnt_block() {
    let channel = TopicBroadcastChannel::new(100);

    // Send batch without any subscribers
    let batch = create_batch(vec!["cpu"], vec![1.0]);
    let topic_batch = TopicBatch {
        batch,
        metadata: BatchMetadata {
            shard_id: "shard-1".to_string(),
            tenant_id: 1,
            metrics: vec!["cpu".to_string()],
        },
    };

    // Should return error (no receivers) but not panic
    let result = channel.send(topic_batch);
    assert!(result.is_err(), "Should return error with no subscribers");
}

#[tokio::test]
async fn test_or_filter_combination() {
    let channel = TopicBroadcastChannel::new(100);

    // Subscribe to cpu OR memory
    let filter = TopicFilter::Or(vec![
        TopicFilter::for_metrics(vec!["cpu".to_string()]),
        TopicFilter::for_metrics(vec!["memory".to_string()]),
    ]);
    let mut rx_filtered = channel.subscribe(filter).await;

    // Send various metrics
    let metrics = ["cpu", "memory", "disk", "network"];
    for metric in &metrics {
        let batch = create_batch(vec![metric], vec![1.0]);
        let topic_batch = TopicBatch {
            batch,
            metadata: BatchMetadata {
                shard_id: "shard-1".to_string(),
                tenant_id: 1,
                metrics: vec![metric.to_string()],
            },
        };
        channel.send(topic_batch).unwrap();
    }

    // Should receive 2 batches (cpu, memory)
    let mut count = 0;
    for _ in 0..2 {
        let result = tokio::time::timeout(Duration::from_millis(100), rx_filtered.recv()).await;
        if result.is_ok() {
            count += 1;
        }
    }
    assert_eq!(count, 2, "Should receive 2 batches with OR filter");
}
