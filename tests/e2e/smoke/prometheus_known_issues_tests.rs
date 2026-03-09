//! Prometheus API tests for known-buggy endpoints
//!
//! These tests exercise Prometheus API features that have known bugs.
//! They are excluded from the default E2E CI run via a separate ignore tag
//! so they don't block merges. Run them explicitly to check if bugs are fixed:
//!
//! ```bash
//! cargo test --test e2e_smoke known_issues -- --ignored --nocapture
//! ```
//!
//! Known issues tracked:
//! - cardinalsin-o75: Prometheus range query fails (value_i64 column not found)
//! - cardinalsin-rqg: /api/v1/series fails for single match[] (serde Vec vs String)
//! - cardinalsin-vsg: /api/v1/label/__name__/values always empty
//! - PromQL label selectors and aggregation functions not fully implemented

use crate::e2e::{generate_test_samples, E2EHarness};
use std::time::Duration;

/// Test Prometheus range query endpoint
///
/// KNOWN ISSUE: value_i64 column not found (cardinalsin-o75)
#[tokio::test]
#[ignore = "known issue: cardinalsin-o75 range query value_i64 schema mismatch"]
async fn test_known_issues_prometheus_range_query() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    let result = harness
        .query_prom_range("test_metric_0", one_hour_ago, now, 60.0)
        .await;

    match result {
        Ok(r) => {
            println!(
                "Prometheus range query returned status: {}, {} series",
                r.status,
                r.data.len()
            );
            assert_eq!(r.status, "success", "Status should be 'success'");
        }
        Err(e) => {
            panic!("Prometheus range query failed: {}", e);
        }
    }
}

/// Test Prometheus range query POST endpoint
///
/// KNOWN ISSUE: value_i64 column not found (cardinalsin-o75)
#[tokio::test]
#[ignore = "known issue: cardinalsin-o75 range query value_i64 schema mismatch"]
async fn test_known_issues_prometheus_range_query_post() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    let result = harness
        .query_prom_range_post("test_metric_0", one_hour_ago, now, 60.0)
        .await;

    match result {
        Ok(r) => {
            println!(
                "Prometheus POST range query returned status: {}, {} series",
                r.status,
                r.data.len()
            );
            assert_eq!(r.status, "success", "Status should be 'success'");
        }
        Err(e) => {
            panic!("Prometheus POST range query failed: {}", e);
        }
    }
}

/// Test Prometheus query with label selectors
///
/// KNOWN ISSUE: PromQL label selector parsing incomplete
#[tokio::test]
#[ignore = "known issue: PromQL label selector parsing incomplete"]
async fn test_known_issues_prometheus_query_with_labels() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let samples = generate_test_samples(20, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let result = harness
        .query_prom(r#"test_metric_0{host="host-001"}"#)
        .await;

    match result {
        Ok(r) => {
            println!(
                "Prometheus query with labels returned: {} results",
                r.data.len()
            );
            assert_eq!(r.status, "success");
        }
        Err(e) => {
            panic!("Prometheus query with labels failed: {}", e);
        }
    }
}

/// Test Prometheus POST query with label selectors
///
/// KNOWN ISSUE: PromQL label selector parsing incomplete
#[tokio::test]
#[ignore = "known issue: PromQL label selector parsing incomplete"]
async fn test_known_issues_prometheus_post_with_labels() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let samples = generate_test_samples(20, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let result = harness
        .query_prom_post(r#"test_metric_0{host="host-001"}"#)
        .await;

    match result {
        Ok(r) => {
            println!(
                "Prometheus POST query with labels returned: {} results",
                r.data.len()
            );
            assert_eq!(r.status, "success");
        }
        Err(e) => {
            panic!("Prometheus POST query with labels failed: {}", e);
        }
    }
}

/// Test Prometheus aggregation query (sum)
///
/// KNOWN ISSUE: PromQL aggregation functions not fully implemented
#[tokio::test]
#[ignore = "known issue: PromQL aggregation functions not implemented"]
async fn test_known_issues_prometheus_sum_aggregation() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let result = harness.query_prom("sum(test_metric_0)").await;

    match result {
        Ok(r) => {
            println!(
                "Prometheus sum aggregation returned: {} results",
                r.data.len()
            );
            assert_eq!(r.status, "success");
        }
        Err(e) => {
            panic!("Prometheus sum aggregation failed: {}", e);
        }
    }
}

/// Test Prometheus aggregation with grouping
///
/// KNOWN ISSUE: PromQL aggregation functions not fully implemented
#[tokio::test]
#[ignore = "known issue: PromQL aggregation functions not implemented"]
async fn test_known_issues_prometheus_sum_by_aggregation() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let result = harness.query_prom("sum by (host)(test_metric_0)").await;

    match result {
        Ok(r) => {
            println!(
                "Prometheus sum by aggregation returned: {} results",
                r.data.len()
            );
            assert_eq!(r.status, "success");
        }
        Err(e) => {
            panic!("Prometheus sum by aggregation failed: {}", e);
        }
    }
}

/// Test /api/v1/series endpoint (GET)
///
/// KNOWN ISSUE: serde expects sequence for single match[] param (cardinalsin-rqg)
#[tokio::test]
#[ignore = "known issue: cardinalsin-rqg series match[] serde"]
async fn test_known_issues_prometheus_series_endpoint() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let samples = generate_test_samples(10, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let resp = harness
        .http_client
        .get(format!("{}/api/v1/series", harness.query_url))
        .query(&[("match[]", "test_metric_0")])
        .send()
        .await
        .expect("Series request should succeed");

    assert!(
        resp.status().is_success(),
        "Series endpoint should return 2xx"
    );

    let body: serde_json::Value = resp.json().await.expect("Should be valid JSON");
    assert_eq!(body["status"], "success");
    assert!(body["data"].is_array());

    println!("Series endpoint returned: {:?}", body["data"]);
}

/// Test /api/v1/series endpoint with time range filtering
///
/// KNOWN ISSUE: serde expects sequence for single match[] param (cardinalsin-rqg)
#[tokio::test]
#[ignore = "known issue: cardinalsin-rqg series match[] serde"]
async fn test_known_issues_prometheus_series_with_time_range() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    let resp = harness
        .http_client
        .get(format!("{}/api/v1/series", harness.query_url))
        .query(&[
            ("match[]", "test_metric_0"),
            ("start", &one_hour_ago.to_string()),
            ("end", &now.to_string()),
        ])
        .send()
        .await
        .expect("Series request should succeed");

    assert!(resp.status().is_success());

    let body: serde_json::Value = resp.json().await.expect("Should be valid JSON");
    assert_eq!(body["status"], "success");
    assert!(body["data"].is_array());

    println!(
        "Series endpoint with time range returned: {} series",
        body["data"].as_array().unwrap().len()
    );
}

/// Test /api/v1/series endpoint (POST)
///
/// KNOWN ISSUE: serde expects sequence for single match[] param (cardinalsin-rqg)
#[tokio::test]
#[ignore = "known issue: cardinalsin-rqg series match[] serde"]
async fn test_known_issues_prometheus_series_post() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let samples = generate_test_samples(10, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let params = [("match[]", "test_metric_0")];
    let resp = harness
        .http_client
        .post(format!("{}/api/v1/series", harness.query_url))
        .form(&params)
        .send()
        .await
        .expect("Series POST request should succeed");

    assert!(
        resp.status().is_success(),
        "Series POST endpoint should return 2xx"
    );

    let body: serde_json::Value = resp.json().await.expect("Should be valid JSON");
    assert_eq!(body["status"], "success");
    assert!(body["data"].is_array());

    println!("Series POST endpoint returned: {:?}", body["data"]);
}

/// Test /api/v1/labels endpoint with POST and filtering
///
/// KNOWN ISSUE: match[] serde parsing (cardinalsin-rqg)
#[tokio::test]
#[ignore = "known issue: cardinalsin-rqg match[] serde"]
async fn test_known_issues_prometheus_labels_post_with_filters() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let samples = generate_test_samples(10, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    let params = [
        ("match[]", "test_metric_0"),
        ("start", &one_hour_ago.to_string()),
        ("end", &now.to_string()),
    ];
    let resp = harness
        .http_client
        .post(format!("{}/api/v1/labels", harness.query_url))
        .form(&params)
        .send()
        .await
        .expect("Labels POST request should succeed");

    assert!(
        resp.status().is_success(),
        "Labels POST endpoint should return 2xx"
    );

    let body: serde_json::Value = resp.json().await.expect("Should be valid JSON");
    assert_eq!(body["status"], "success");
    assert!(body["data"].is_array());

    println!("Labels POST with filters returned: {:?}", body["data"]);
}

/// Test /api/v1/label/{name}/values endpoint with filtering
///
/// KNOWN ISSUE: __name__ values always empty (cardinalsin-vsg)
#[tokio::test]
#[ignore = "known issue: cardinalsin-vsg __name__ values always empty"]
async fn test_known_issues_prometheus_label_values_with_filters() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let samples = generate_test_samples(10, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    let resp = harness
        .http_client
        .get(format!(
            "{}/api/v1/label/__name__/values",
            harness.query_url
        ))
        .query(&[
            ("match[]", "test_metric_0"),
            ("start", &one_hour_ago.to_string()),
            ("end", &now.to_string()),
        ])
        .send()
        .await
        .expect("Label values request should succeed");

    assert!(
        resp.status().is_success(),
        "Label values endpoint should return 2xx"
    );

    let body: serde_json::Value = resp.json().await.expect("Should be valid JSON");
    assert_eq!(body["status"], "success");
    assert!(body["data"].is_array());

    let values = body["data"].as_array().unwrap();
    assert!(
        !values.is_empty(),
        "__name__ label values should not be empty"
    );

    println!("Label values with filters returned: {:?}", body["data"]);
}
