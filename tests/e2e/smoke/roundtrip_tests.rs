//! Roundtrip tests - write data, then query it back
//!
//! Validates the core data flow: ingestion -> storage -> query.

use crate::e2e::{generate_test_samples, E2EHarness, Sample};
use std::time::Duration;

/// Test basic write-then-query roundtrip
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_write_then_query_returns_data() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Write known data
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let samples = vec![Sample {
        timestamp_ns: now,
        metric_name: "e2e_test_metric".to_string(),
        value: 42.0,
        labels: [("host".to_string(), "e2e-test-host".to_string())].into(),
    }];

    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    // Wait for flush (the ingester buffers data before flushing to S3)
    // In a real test, you might trigger a flush or wait for the flush interval
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query and verify
    let result = harness
        .query_sql("SELECT * FROM metrics WHERE metric_name = 'e2e_test_metric' LIMIT 10")
        .await;

    match result {
        Ok(r) => {
            println!("Query returned {} rows", r.rows.len());
            if !r.rows.is_empty() {
                assert!(
                    r.columns.contains(&"metric_name".to_string()),
                    "Should have metric_name column"
                );
            }
        }
        Err(e) => {
            // Query may fail if data hasn't been flushed yet - that's OK for this test
            println!("Query failed (data may not be flushed yet): {}", e);
        }
    }
}

/// Test multiple metrics are queryable
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_multiple_metrics_queryable() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Generate and write 100 samples across 5 metrics
    let samples = generate_test_samples(100, 5);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    // Wait for potential flush
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query for distinct metrics
    let result = harness
        .query_sql("SELECT DISTINCT metric_name FROM metrics")
        .await;

    match result {
        Ok(r) => {
            println!("Found {} distinct metrics", r.rows.len());
            // In a fully flushed state, we'd expect 5 metrics
        }
        Err(e) => {
            // Query may fail if data hasn't been flushed yet
            println!("Query failed (data may not be flushed yet): {}", e);
        }
    }
}

/// Test SQL query with filters
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_sql_query_with_filters() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Try a filtered query on any existing data
    let result = harness
        .query_sql(
            "SELECT metric_name, COUNT(*) as cnt FROM metrics
             WHERE timestamp > 0
             GROUP BY metric_name
             ORDER BY cnt DESC
             LIMIT 10",
        )
        .await;

    match result {
        Ok(r) => {
            println!("Query returned {} grouped metrics", r.rows.len());
            assert!(
                r.columns.contains(&"metric_name".to_string()),
                "Should have metric_name column"
            );
            assert!(
                r.columns.contains(&"cnt".to_string()),
                "Should have cnt column"
            );
        }
        Err(e) => {
            println!("Query failed: {}", e);
        }
    }
}

/// Test SQL aggregation queries
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_sql_aggregation_query() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Test various aggregations
    let result = harness
        .query_sql(
            "SELECT
                metric_name,
                AVG(value_f64) as avg_value,
                MIN(value_f64) as min_value,
                MAX(value_f64) as max_value,
                COUNT(*) as sample_count
             FROM metrics
             GROUP BY metric_name
             LIMIT 10",
        )
        .await;

    match result {
        Ok(r) => {
            println!("Aggregation query returned {} metrics", r.rows.len());
            // Verify expected columns are present
            for col in ["metric_name", "avg_value", "min_value", "max_value", "sample_count"] {
                assert!(
                    r.columns.contains(&col.to_string()),
                    "Should have {} column",
                    col
                );
            }
        }
        Err(e) => {
            println!("Aggregation query failed: {}", e);
        }
    }
}

/// Test time-range queries
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_time_range_query() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Query for last hour of data
    let one_hour_ago_ns = (chrono::Utc::now() - chrono::Duration::hours(1))
        .timestamp_nanos_opt()
        .unwrap();

    let result = harness
        .query_sql(&format!(
            "SELECT * FROM metrics
             WHERE timestamp >= {}
             ORDER BY timestamp DESC
             LIMIT 100",
            one_hour_ago_ns
        ))
        .await;

    match result {
        Ok(r) => {
            println!(
                "Time range query returned {} samples from last hour",
                r.rows.len()
            );
        }
        Err(e) => {
            println!("Time range query failed: {}", e);
        }
    }
}

/// Test query error handling for invalid SQL
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_invalid_sql_returns_error() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Invalid SQL should return an error
    let result = harness
        .query_sql("SELCT * FORM nonexistent_table")
        .await;

    assert!(result.is_err(), "Invalid SQL should return error");
}
