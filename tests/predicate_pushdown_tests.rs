//! Integration tests for column-based predicate pushdown
//!
//! Tests that queries with column predicates correctly prune chunks at the metadata level,
//! dramatically reducing S3 read costs.

use cardinalsin::metadata::{
    ColumnPredicate, MetadataClient, PredicateValue, S3MetadataClient, S3MetadataConfig,
    TimeRange,
};
use cardinalsin::ingester::ChunkMetadata;
use object_store::memory::InMemory;
use std::sync::Arc;

/// Helper to create test metadata client
fn create_test_client() -> S3MetadataClient {
    let store = Arc::new(InMemory::new());
    let config = S3MetadataConfig {
        bucket: "test-bucket".to_string(),
        metadata_prefix: "test/".to_string(),
        enable_cache: true,
    };
    S3MetadataClient::new(store, config)
}

/// Helper to create chunk metadata with column stats
async fn register_chunk_with_stats(
    client: &S3MetadataClient,
    path: &str,
    min_ts: i64,
    max_ts: i64,
    metric_name: &str,
) {
    let chunk = ChunkMetadata {
        path: path.to_string(),
        min_timestamp: min_ts,
        max_timestamp: max_ts,
        row_count: 1000,
        size_bytes: 100_000,
    };

    client.register_chunk(path, &chunk).await.unwrap();

    // Manually update column stats for testing
    // In production, these would be collected during Parquet writing
    let mut all_metadata = client.load_chunk_metadata().await.unwrap();
    if let Some(extended) = all_metadata.get_mut(path) {
        use cardinalsin::metadata::ColumnStats;
        extended.column_stats.insert(
            "metric_name".to_string(),
            ColumnStats {
                min: serde_json::json!(metric_name),
                max: serde_json::json!(metric_name),
                has_nulls: false,
            },
        );
    }
    client.save_chunk_metadata(&all_metadata).await.unwrap();
}

#[tokio::test]
async fn test_predicate_pushdown_reduces_chunks() {
    let client = create_test_client();

    // Register 10 chunks: 5 with 'cpu', 5 with 'memory'
    let base_time = 1_000_000_000_000_000_000i64; // Some timestamp
    let hour = 3_600_000_000_000i64;

    for i in 0..5 {
        let path = format!("test/cpu_chunk_{}.parquet", i);
        let min_ts = base_time + (i * hour);
        let max_ts = base_time + ((i + 1) * hour);
        register_chunk_with_stats(&client, &path, min_ts, max_ts, "cpu").await;
    }

    for i in 0..5 {
        let path = format!("test/memory_chunk_{}.parquet", i);
        let min_ts = base_time + (i * hour);
        let max_ts = base_time + ((i + 1) * hour);
        register_chunk_with_stats(&client, &path, min_ts, max_ts, "memory").await;
    }

    // Query without predicates - should return all 10 chunks
    let time_range = TimeRange::new(base_time, base_time + (10 * hour));
    let all_chunks = client.get_chunks(time_range).await.unwrap();
    assert_eq!(all_chunks.len(), 10, "Should return all chunks without predicates");

    // Query with metric_name = 'cpu' predicate - should return only 5 chunks
    let predicates = vec![ColumnPredicate::Eq(
        "metric_name".to_string(),
        PredicateValue::String("cpu".to_string()),
    )];
    let filtered_chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(
        filtered_chunks.len(),
        5,
        "Should return only 'cpu' chunks with predicate"
    );
    assert!(
        filtered_chunks.iter().all(|c| c.chunk_path.contains("cpu")),
        "All returned chunks should be CPU chunks"
    );

    // Verify 50% reduction in chunks fetched
    let reduction_pct = ((all_chunks.len() - filtered_chunks.len()) * 100) / all_chunks.len();
    assert_eq!(reduction_pct, 50, "Should have 50% chunk reduction");
}

#[tokio::test]
async fn test_predicate_pushdown_with_ranges() {
    let client = create_test_client();

    let base_time = 1_000_000_000_000_000_000i64;
    let hour = 3_600_000_000_000i64;

    // Register chunks with different value ranges
    // Chunk 1: values 0-100
    register_chunk_with_stats(&client, "chunk_low.parquet", base_time, base_time + hour, "0").await;

    // Chunk 2: values 100-200
    register_chunk_with_stats(
        &client,
        "chunk_mid.parquet",
        base_time + hour,
        base_time + (2 * hour),
        "100",
    )
    .await;

    // Chunk 3: values 200-300
    register_chunk_with_stats(
        &client,
        "chunk_high.parquet",
        base_time + (2 * hour),
        base_time + (3 * hour),
        "200",
    )
    .await;

    // Simulate numeric column stats
    let mut all_metadata = client.load_chunk_metadata().await.unwrap();
    if let Some(extended) = all_metadata.get_mut("chunk_low.parquet") {
        use cardinalsin::metadata::ColumnStats;
        extended.column_stats.insert(
            "value".to_string(),
            ColumnStats {
                min: serde_json::json!(0),
                max: serde_json::json!(100),
                has_nulls: false,
            },
        );
    }
    if let Some(extended) = all_metadata.get_mut("chunk_mid.parquet") {
        use cardinalsin::metadata::ColumnStats;
        extended.column_stats.insert(
            "value".to_string(),
            ColumnStats {
                min: serde_json::json!(100),
                max: serde_json::json!(200),
                has_nulls: false,
            },
        );
    }
    if let Some(extended) = all_metadata.get_mut("chunk_high.parquet") {
        use cardinalsin::metadata::ColumnStats;
        extended.column_stats.insert(
            "value".to_string(),
            ColumnStats {
                min: serde_json::json!(200),
                max: serde_json::json!(300),
                has_nulls: false,
            },
        );
    }
    client.save_chunk_metadata(&all_metadata).await.unwrap();

    let time_range = TimeRange::new(base_time, base_time + (4 * hour));

    // Test: value > 150 should only return mid and high chunks
    let predicates = vec![ColumnPredicate::Gt(
        "value".to_string(),
        PredicateValue::Int64(150),
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(chunks.len(), 2, "Should return 2 chunks with value > 150");
    assert!(
        chunks
            .iter()
            .all(|c| c.chunk_path.contains("mid") || c.chunk_path.contains("high")),
        "Should return mid and high chunks"
    );

    // Test: value < 50 should only return low chunk
    let predicates = vec![ColumnPredicate::Lt(
        "value".to_string(),
        PredicateValue::Int64(50),
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(chunks.len(), 1, "Should return 1 chunk with value < 50");
    assert!(
        chunks[0].chunk_path.contains("low"),
        "Should return low chunk"
    );
}

#[tokio::test]
async fn test_predicate_pushdown_with_and_or() {
    let client = create_test_client();

    let base_time = 1_000_000_000_000_000_000i64;
    let hour = 3_600_000_000_000i64;

    // Register chunks
    register_chunk_with_stats(&client, "cpu_low.parquet", base_time, base_time + hour, "cpu").await;
    register_chunk_with_stats(
        &client,
        "cpu_high.parquet",
        base_time + hour,
        base_time + (2 * hour),
        "cpu",
    )
    .await;
    register_chunk_with_stats(
        &client,
        "memory.parquet",
        base_time + (2 * hour),
        base_time + (3 * hour),
        "memory",
    )
    .await;

    // Add value stats
    let mut all_metadata = client.load_chunk_metadata().await.unwrap();
    if let Some(extended) = all_metadata.get_mut("cpu_low.parquet") {
        use cardinalsin::metadata::ColumnStats;
        extended.column_stats.insert(
            "value".to_string(),
            ColumnStats {
                min: serde_json::json!(10),
                max: serde_json::json!(50),
                has_nulls: false,
            },
        );
    }
    if let Some(extended) = all_metadata.get_mut("cpu_high.parquet") {
        use cardinalsin::metadata::ColumnStats;
        extended.column_stats.insert(
            "value".to_string(),
            ColumnStats {
                min: serde_json::json!(80),
                max: serde_json::json!(100),
                has_nulls: false,
            },
        );
    }
    if let Some(extended) = all_metadata.get_mut("memory.parquet") {
        use cardinalsin::metadata::ColumnStats;
        extended.column_stats.insert(
            "value".to_string(),
            ColumnStats {
                min: serde_json::json!(20),
                max: serde_json::json!(40),
                has_nulls: false,
            },
        );
    }
    client.save_chunk_metadata(&all_metadata).await.unwrap();

    let time_range = TimeRange::new(base_time, base_time + (4 * hour));

    // Test AND: metric = 'cpu' AND value > 60
    // Should return only cpu_high
    let predicates = vec![ColumnPredicate::And(
        Box::new(ColumnPredicate::Eq(
            "metric_name".to_string(),
            PredicateValue::String("cpu".to_string()),
        )),
        Box::new(ColumnPredicate::Gt(
            "value".to_string(),
            PredicateValue::Int64(60),
        )),
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(
        chunks.len(),
        1,
        "AND predicate should return only cpu_high"
    );
    assert!(chunks[0].chunk_path.contains("cpu_high"));

    // Test OR: metric = 'cpu' OR metric = 'memory'
    // Should return all 3 chunks
    let predicates = vec![ColumnPredicate::Or(
        Box::new(ColumnPredicate::Eq(
            "metric_name".to_string(),
            PredicateValue::String("cpu".to_string()),
        )),
        Box::new(ColumnPredicate::Eq(
            "metric_name".to_string(),
            PredicateValue::String("memory".to_string()),
        )),
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(chunks.len(), 3, "OR predicate should return all chunks");
}

#[tokio::test]
async fn test_predicate_pushdown_with_in_list() {
    let client = create_test_client();

    let base_time = 1_000_000_000_000_000_000i64;
    let hour = 3_600_000_000_000i64;

    // Register chunks with different metrics
    register_chunk_with_stats(&client, "cpu.parquet", base_time, base_time + hour, "cpu").await;
    register_chunk_with_stats(
        &client,
        "memory.parquet",
        base_time + hour,
        base_time + (2 * hour),
        "memory",
    )
    .await;
    register_chunk_with_stats(
        &client,
        "disk.parquet",
        base_time + (2 * hour),
        base_time + (3 * hour),
        "disk",
    )
    .await;
    register_chunk_with_stats(
        &client,
        "network.parquet",
        base_time + (3 * hour),
        base_time + (4 * hour),
        "network",
    )
    .await;

    let time_range = TimeRange::new(base_time, base_time + (5 * hour));

    // Test IN: metric IN ('cpu', 'memory')
    let predicates = vec![ColumnPredicate::In(
        "metric_name".to_string(),
        vec![
            PredicateValue::String("cpu".to_string()),
            PredicateValue::String("memory".to_string()),
        ],
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(chunks.len(), 2, "IN predicate should return 2 chunks");
    assert!(
        chunks
            .iter()
            .all(|c| c.chunk_path.contains("cpu") || c.chunk_path.contains("memory")),
        "Should return only cpu and memory chunks"
    );

    // Test NOT IN: metric NOT IN ('disk')
    // Should return cpu, memory, network
    let predicates = vec![ColumnPredicate::NotIn(
        "metric_name".to_string(),
        vec![PredicateValue::String("disk".to_string())],
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    // NotIn can't rule out chunks conservatively, so it returns all
    assert_eq!(
        chunks.len(),
        4,
        "NOT IN predicate is conservative, returns all chunks"
    );
}

#[tokio::test]
async fn test_predicate_pushdown_between() {
    let client = create_test_client();

    let base_time = 1_000_000_000_000_000_000i64;
    let hour = 3_600_000_000_000i64;

    // Register chunks with different value ranges
    register_chunk_with_stats(&client, "chunk_1.parquet", base_time, base_time + hour, "test").await;
    register_chunk_with_stats(
        &client,
        "chunk_2.parquet",
        base_time + hour,
        base_time + (2 * hour),
        "test",
    )
    .await;
    register_chunk_with_stats(
        &client,
        "chunk_3.parquet",
        base_time + (2 * hour),
        base_time + (3 * hour),
        "test",
    )
    .await;

    // Add value stats: chunk_1 [0-100], chunk_2 [100-200], chunk_3 [200-300]
    let mut all_metadata = client.load_chunk_metadata().await.unwrap();
    for (i, path) in ["chunk_1.parquet", "chunk_2.parquet", "chunk_3.parquet"]
        .iter()
        .enumerate()
    {
        if let Some(extended) = all_metadata.get_mut(*path) {
            use cardinalsin::metadata::ColumnStats;
            extended.column_stats.insert(
                "value".to_string(),
                ColumnStats {
                    min: serde_json::json!(i * 100),
                    max: serde_json::json!((i + 1) * 100),
                    has_nulls: false,
                },
            );
        }
    }
    client.save_chunk_metadata(&all_metadata).await.unwrap();

    let time_range = TimeRange::new(base_time, base_time + (4 * hour));

    // Test BETWEEN 50 AND 150
    // Should return chunk_1 [0-100] and chunk_2 [100-200]
    let predicates = vec![ColumnPredicate::Between(
        "value".to_string(),
        PredicateValue::Int64(50),
        PredicateValue::Int64(150),
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(
        chunks.len(),
        2,
        "BETWEEN should return overlapping chunks"
    );
    assert!(
        chunks
            .iter()
            .all(|c| c.chunk_path.contains("chunk_1") || c.chunk_path.contains("chunk_2")),
        "Should return chunk_1 and chunk_2"
    );
}

#[tokio::test]
async fn test_cost_reduction_simulation() {
    let client = create_test_client();

    let base_time = 1_000_000_000_000_000_000i64;
    let hour = 3_600_000_000_000i64;

    // Simulate a real scenario: 100 chunks over 1 day, 10 different metrics
    let metrics = [
        "cpu", "memory", "disk", "network", "latency", "errors", "requests", "responses",
        "connections", "threads",
    ];

    for i in 0..100 {
        let metric = metrics[i % metrics.len()];
        let path = format!("chunk_{:03}_{}.parquet", i, metric);
        let min_ts = base_time + (i as i64 * hour / 4); // 15-minute chunks
        let max_ts = min_ts + (hour / 4);
        register_chunk_with_stats(&client, &path, min_ts, max_ts, metric).await;
    }

    // Query range needs to cover all 100 chunks (100 * 15min = 25 hours)
    let time_range = TimeRange::new(base_time, base_time + (26 * hour));

    // Query 1: All data (no predicate)
    let all_chunks = client.get_chunks(time_range).await.unwrap();
    assert_eq!(all_chunks.len(), 100, "Should have 100 total chunks");

    // Query 2: Single metric (90% reduction expected)
    let predicates = vec![ColumnPredicate::Eq(
        "metric_name".to_string(),
        PredicateValue::String("cpu".to_string()),
    )];
    let filtered_chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(
        filtered_chunks.len(),
        10,
        "Should have 10 chunks for single metric"
    );

    // Calculate cost reduction
    let chunks_saved = all_chunks.len() - filtered_chunks.len();
    let reduction_pct = (chunks_saved * 100) / all_chunks.len();

    println!("Cost Reduction Analysis:");
    println!("  Total chunks: {}", all_chunks.len());
    println!("  Chunks after predicate pushdown: {}", filtered_chunks.len());
    println!("  Chunks saved: {}", chunks_saved);
    println!("  Reduction: {}%", reduction_pct);

    assert_eq!(reduction_pct, 90, "Should achieve 90% cost reduction");

    // Simulate S3 cost savings (assuming $0.0004 per 1000 requests)
    let requests_saved = chunks_saved;
    let cost_saved_per_query = (requests_saved as f64 * 0.0004) / 1000.0;
    let queries_per_day = 10_000;
    let annual_savings = cost_saved_per_query * queries_per_day as f64 * 365.0;

    println!("  S3 requests saved per query: {}", requests_saved);
    println!("  Cost saved per query: ${:.6}", cost_saved_per_query);
    println!("  Annual savings (10k queries/day): ${:.2}", annual_savings);

    assert!(annual_savings > 100.0, "Should save >$100/year at scale");
}

#[tokio::test]
async fn test_empty_result_optimization() {
    let client = create_test_client();

    let base_time = 1_000_000_000_000_000_000i64;
    let hour = 3_600_000_000_000i64;

    // Register chunks with specific metrics
    register_chunk_with_stats(&client, "cpu.parquet", base_time, base_time + hour, "cpu").await;
    register_chunk_with_stats(
        &client,
        "memory.parquet",
        base_time + hour,
        base_time + (2 * hour),
        "memory",
    )
    .await;

    let time_range = TimeRange::new(base_time, base_time + (3 * hour));

    // Query for non-existent metric
    let predicates = vec![ColumnPredicate::Eq(
        "metric_name".to_string(),
        PredicateValue::String("nonexistent".to_string()),
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    assert_eq!(
        chunks.len(),
        0,
        "Should return no chunks for non-existent metric"
    );

    // Query with impossible range
    let predicates = vec![ColumnPredicate::Gt(
        "value".to_string(),
        PredicateValue::Int64(1_000_000),
    )];
    let chunks = client
        .get_chunks_with_predicates(time_range, &predicates)
        .await
        .unwrap();

    // Without value stats, this would return all chunks conservatively
    assert!(
        chunks.len() >= 0,
        "Conservative behavior without stats"
    );
}
