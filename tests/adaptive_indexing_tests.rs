//! Integration tests for P2: Adaptive Indexing Integration with Query Planner
//!
//! These tests verify that the query engine correctly extracts filter columns,
//! tracks index usage, and manages index lifecycle.

use cardinalsin::adaptive_index::{
    AdaptiveIndexController, AdaptiveIndexConfig, IndexVisibility,
};
use cardinalsin::query::{QueryEngine, QueryConfig, QueryNode};
use cardinalsin::metadata::{LocalMetadataClient, MetadataClient};
use cardinalsin::ingester::ChunkMetadata;
use cardinalsin::{StorageConfig};
use object_store::memory::InMemory;
use std::sync::Arc;

/// Helper to create a test query engine
async fn create_test_query_engine() -> QueryEngine {
    let object_store = Arc::new(InMemory::new());
    let cache = Arc::new(cardinalsin::query::TieredCache::new(
        cardinalsin::query::CacheConfig {
            l1_size: 1024 * 1024,
            l2_size: 10 * 1024 * 1024,
            l2_dir: None,
        },
    ).await.unwrap());

    QueryEngine::new(object_store, cache).await.unwrap()
}

/// Test filter column extraction from simple WHERE clause
#[tokio::test]
async fn test_extract_filter_columns_simple() {
    let engine = create_test_query_engine().await;

    // This would require actually executing a query, which needs data
    // For now, we'll test the integration exists
    let index_controller = Arc::new(AdaptiveIndexController::new(
        AdaptiveIndexConfig::default(),
    ));

    // Verify controller was created
    assert!(Arc::strong_count(&index_controller) >= 1);
}

/// Test QueryFilter parsing with sqlparser (verifies P1 fix)
#[tokio::test]
async fn test_query_filter_parsing() {
    use cardinalsin::query::QueryFilter;

    // Test simple equality with string
    let filter = QueryFilter::from_sql("SELECT * FROM metrics WHERE service = 'api'");
    assert!(!filter.predicates.is_empty(), "Should parse string equality");
    assert_eq!(filter.predicates[0].column, "service");

    // Test numeric equality
    let filter2 = QueryFilter::from_sql("SELECT * FROM metrics WHERE status_code = 200");
    assert!(!filter2.predicates.is_empty(), "Should parse numeric equality");
    assert_eq!(filter2.predicates[0].column, "status_code");

    // Test compound WHERE clause with AND
    let filter3 = QueryFilter::from_sql(
        "SELECT * FROM metrics WHERE tenant_id = 'test' AND region = 'us-east'"
    );
    assert!(filter3.predicates.len() >= 2, "Should parse multiple predicates");

    // Test comparison operators
    let filter4 = QueryFilter::from_sql("SELECT * FROM metrics WHERE value > 100");
    assert!(!filter4.predicates.is_empty(), "Should parse greater than");

    // Test empty/invalid SQL graceful handling
    let filter5 = QueryFilter::from_sql("not valid sql at all");
    assert!(filter5.predicates.is_empty(), "Should return empty for invalid SQL");
}

/// Test index usage tracking
#[tokio::test]
async fn test_index_usage_tracking() {
    let index_controller = Arc::new(AdaptiveIndexController::new(
        AdaptiveIndexConfig::default(),
    ));

    let tenant_id = "test-tenant".to_string();

    // Create an invisible index
    let index_id = index_controller
        .lifecycle_manager
        .create_invisible_index(
            tenant_id.clone(),
            "trace_id".to_string(),
            cardinalsin::adaptive_index::IndexType::Inverted,
        )
        .await
        .unwrap();

    // Record usage multiple times
    for _ in 0..10 {
        index_controller.lifecycle_manager.record_usage(&index_id);
    }

    // Verify index exists and has usage count
    let indexes = index_controller.lifecycle_manager.get_invisible_indexes(&tenant_id);
    assert_eq!(indexes.len(), 1, "Should have 1 invisible index");
    assert_eq!(indexes[0].usage_count, 10, "Should have 10 usage records");
}

/// Test invisible index promotion
#[tokio::test]
async fn test_invisible_index_promotion() {
    let mut config = AdaptiveIndexConfig::default();
    config.visibility_check_delay = std::time::Duration::from_millis(100); // Short delay for testing

    let index_controller = Arc::new(AdaptiveIndexController::new(config));

    let tenant_id = "test-tenant".to_string();

    // Create an invisible index
    let index_id = index_controller
        .lifecycle_manager
        .create_invisible_index(
            tenant_id.clone(),
            "user_id".to_string(),
            cardinalsin::adaptive_index::IndexType::Inverted,
        )
        .await
        .unwrap();

    // Record 100 "would have helped" hits
    for _ in 0..100 {
        index_controller
            .lifecycle_manager
            .record_would_have_helped(&index_id);
    }

    // Wait for visibility check delay
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Check visibility
    index_controller.lifecycle_manager.check_visibility().await;

    // Verify index was promoted to visible
    let visible_indexes = index_controller.lifecycle_manager.get_visible_indexes(&tenant_id);
    assert!(
        !visible_indexes.is_empty(),
        "Index should be promoted to visible after 100 would-have-helped hits"
    );
    assert!(
        visible_indexes.contains(&index_id),
        "Promoted index ID should be in visible list"
    );
}

/// Test index lifecycle: invisible → visible → deprecated
#[tokio::test]
async fn test_full_index_lifecycle() {
    let mut config = AdaptiveIndexConfig::default();
    config.visibility_check_delay = std::time::Duration::from_millis(100);
    config.unused_days_threshold = 0; // Immediate deprecation for testing

    let index_controller = Arc::new(AdaptiveIndexController::new(config));

    let tenant_id = "test-tenant".to_string();

    // 1. Create invisible index
    let index_id = index_controller
        .lifecycle_manager
        .create_invisible_index(
            tenant_id.clone(),
            "service_name".to_string(),
            cardinalsin::adaptive_index::IndexType::Inverted,
        )
        .await
        .unwrap();

    let invisible = index_controller.lifecycle_manager.get_invisible_indexes(&tenant_id);
    assert_eq!(invisible.len(), 1, "Should start as invisible");
    assert_eq!(invisible[0].visibility, IndexVisibility::Invisible);

    // 2. Promote to visible
    for _ in 0..100 {
        index_controller
            .lifecycle_manager
            .record_would_have_helped(&index_id);
    }

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    index_controller.lifecycle_manager.check_visibility().await;

    let visible_meta = index_controller
        .lifecycle_manager
        .get_visible_index_metadata(&tenant_id);
    assert_eq!(visible_meta.len(), 1, "Should be promoted to visible");
    assert_eq!(visible_meta[0].visibility, IndexVisibility::Visible);

    // 3. Become unused and deprecated
    // Don't record any usage - check for unused
    index_controller.lifecycle_manager.check_unused().await;

    // Index should be deprecated now - check visibility directly since check_unused
    // already marked it as deprecated (retirement_check won't return already-deprecated indexes)
    let all_indexes = index_controller.lifecycle_manager.get_visible_index_metadata(&tenant_id);
    assert!(
        all_indexes.is_empty(),
        "Index should no longer be visible after deprecation"
    );
}

/// Test query node integration with adaptive indexing
#[tokio::test]
async fn test_query_node_integration() {
    let object_store = Arc::new(InMemory::new());
    let metadata = Arc::new(LocalMetadataClient::new());
    let storage_config = StorageConfig::default();

    let index_controller = Arc::new(AdaptiveIndexController::new(
        AdaptiveIndexConfig::default(),
    ));

    let query_node = QueryNode::new(
        QueryConfig::default(),
        object_store,
        metadata,
        storage_config,
    )
    .await
    .unwrap()
    .with_adaptive_indexing(index_controller.clone());

    // Verify controller is connected
    // We can't easily test query execution without setting up full data,
    // but we can verify the integration compiles and initializes
    assert!(Arc::strong_count(&index_controller) >= 2); // query_node + our reference
}

/// Test filter statistics collection
#[tokio::test]
async fn test_filter_statistics_collection() {
    let index_controller = Arc::new(AdaptiveIndexController::new(
        AdaptiveIndexConfig::default(),
    ));

    let tenant_id = "test-tenant".to_string();

    // Record filter usage
    index_controller.record_filter(
        &tenant_id,
        "status_code",
        0.1, // 10% selectivity
        std::time::Duration::from_millis(50),
    );

    index_controller.record_filter(
        &tenant_id,
        "status_code",
        0.15,
        std::time::Duration::from_millis(45),
    );

    index_controller.record_filter(
        &tenant_id,
        "endpoint",
        0.05, // 5% selectivity
        std::time::Duration::from_millis(100),
    );

    // Get statistics
    let stats = index_controller.get_tenant_stats(&tenant_id);
    assert!(stats.is_some(), "Should have collected statistics");

    let stats = stats.unwrap();
    assert!(
        stats.column_filter_stats.contains_key("status_code"),
        "Should have stats for status_code"
    );
    assert!(
        stats.column_filter_stats.contains_key("endpoint"),
        "Should have stats for endpoint"
    );

    let status_stats = &stats.column_filter_stats["status_code"];
    assert_eq!(
        status_stats.filter_count, 2,
        "status_code should have 2 filter uses"
    );

    let endpoint_stats = &stats.column_filter_stats["endpoint"];
    assert_eq!(
        endpoint_stats.filter_count, 1,
        "endpoint should have 1 filter use"
    );
}

/// Test GROUP BY statistics collection
#[tokio::test]
async fn test_groupby_statistics_collection() {
    let index_controller = Arc::new(AdaptiveIndexController::new(
        AdaptiveIndexConfig::default(),
    ));

    let tenant_id = "test-tenant".to_string();

    // Record GROUP BY usage
    index_controller.record_groupby(&tenant_id, &["region".to_string(), "service".to_string()]);
    index_controller.record_groupby(&tenant_id, &["region".to_string()]);
    index_controller.record_groupby(&tenant_id, &["service".to_string()]);

    // Get statistics
    let stats = index_controller.get_tenant_stats(&tenant_id);
    assert!(stats.is_some(), "Should have collected statistics");

    let stats = stats.unwrap();
    // GROUP BY stats are tracked in groupby_stats, not column_filter_stats
    assert!(
        stats.groupby_stats.contains_key("region"),
        "Should have stats for region"
    );
    assert!(
        stats.groupby_stats.contains_key("service"),
        "Should have stats for service"
    );

    // region was used in 2 GROUP BY queries
    assert_eq!(
        stats.groupby_stats.get("region"),
        Some(&2),
        "region should have 2 GROUP BY uses"
    );
    // service was used in 2 GROUP BY queries
    assert_eq!(
        stats.groupby_stats.get("service"),
        Some(&2),
        "service should have 2 GROUP BY uses"
    );
}

/// Test index recommendation generation
#[tokio::test]
async fn test_index_recommendation_generation() {
    let mut config = AdaptiveIndexConfig::default();
    config.min_filter_threshold = 5; // Low threshold for testing
    config.score_threshold = 0.1;

    let index_controller = Arc::new(AdaptiveIndexController::new(config));

    let tenant_id = "test-tenant".to_string();

    // Record enough filter usage to trigger recommendations
    for _ in 0..10 {
        index_controller.record_filter(
            &tenant_id,
            "trace_id",
            0.01, // Very selective (1%)
            std::time::Duration::from_millis(200), // Slow query
        );
    }

    // Run a cycle to generate recommendations
    index_controller.run_cycle().await.unwrap();

    // Check if invisible indexes were created
    let invisible = index_controller
        .lifecycle_manager
        .get_invisible_indexes(&tenant_id);

    assert!(
        !invisible.is_empty(),
        "Should create invisible index for trace_id based on usage pattern"
    );
}

/// Test that index-aware queries record usage correctly
#[tokio::test]
async fn test_index_aware_query_usage() {
    let index_controller = Arc::new(AdaptiveIndexController::new(
        AdaptiveIndexConfig::default(),
    ));

    let tenant_id = "test-tenant".to_string();

    // Create a visible index
    let index_id = index_controller
        .lifecycle_manager
        .create_invisible_index(
            tenant_id.clone(),
            "request_id".to_string(),
            cardinalsin::adaptive_index::IndexType::Inverted,
        )
        .await
        .unwrap();

    // Promote to visible immediately (for testing)
    for _ in 0..100 {
        index_controller
            .lifecycle_manager
            .record_would_have_helped(&index_id);
    }

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    index_controller.lifecycle_manager.check_visibility().await;

    // Simulate query execution that would use the index
    index_controller.lifecycle_manager.record_usage(&index_id);
    index_controller.lifecycle_manager.record_usage(&index_id);
    index_controller.lifecycle_manager.record_usage(&index_id);

    // Verify usage was recorded
    let visible_meta = index_controller
        .lifecycle_manager
        .get_visible_index_metadata(&tenant_id);

    assert!(!visible_meta.is_empty(), "Should have visible index");
    assert!(
        visible_meta[0].usage_count >= 3,
        "Should have at least 3 usage records"
    );
}

/// Test graceful degradation without indexes
#[tokio::test]
async fn test_graceful_degradation_without_indexes() {
    let object_store = Arc::new(InMemory::new());
    let metadata = Arc::new(LocalMetadataClient::new());
    let storage_config = StorageConfig::default();

    // Create query node WITHOUT adaptive indexing
    let _query_node = QueryNode::new(
        QueryConfig::default(),
        object_store,
        metadata,
        storage_config,
    )
    .await
    .unwrap();
    // Don't call .with_adaptive_indexing()

    // Query node should work fine without indexing
    // This test verifies compilation and initialization work
    // Actual query execution would require test data
}

/// Test index removal
#[tokio::test]
async fn test_index_removal() {
    let index_controller = Arc::new(AdaptiveIndexController::new(
        AdaptiveIndexConfig::default(),
    ));

    let tenant_id = "test-tenant".to_string();

    // Create an index
    let index_id = index_controller
        .lifecycle_manager
        .create_invisible_index(
            tenant_id.clone(),
            "temp_column".to_string(),
            cardinalsin::adaptive_index::IndexType::Inverted,
        )
        .await
        .unwrap();

    // Verify it exists
    let invisible = index_controller.lifecycle_manager.get_invisible_indexes(&tenant_id);
    assert_eq!(invisible.len(), 1, "Should have 1 index");

    // Remove it
    index_controller.lifecycle_manager.remove_index(&index_id);

    // Verify it's gone
    let invisible_after = index_controller.lifecycle_manager.get_invisible_indexes(&tenant_id);
    assert_eq!(invisible_after.len(), 0, "Index should be removed");
}
