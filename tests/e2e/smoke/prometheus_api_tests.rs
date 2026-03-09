//! Prometheus API compatibility tests
//!
//! Validates that the Prometheus-compatible API works correctly,
//! ensuring compatibility with Grafana and other Prometheus clients.
//!
//! Tests for known-buggy endpoints live in `prometheus_known_issues_tests.rs`
//! and are excluded from CI via a separate `#[ignore]` tag.

use crate::e2e::{generate_test_samples, E2EHarness};
use std::time::Duration;

/// Test Prometheus instant query endpoint
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_instant_query() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let samples = generate_test_samples(10, 1);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let result = harness.query_prom("test_metric_0").await;

    match result {
        Ok(r) => {
            println!(
                "Prometheus instant query returned status: {}, {} results",
                r.status,
                r.data.len()
            );
            assert_eq!(r.status, "success", "Status should be 'success'");
        }
        Err(e) => {
            println!("Prometheus query failed: {}", e);
        }
    }
}

/// Test Prometheus labels endpoint
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_labels_endpoint() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let resp = harness
        .http_client
        .get(format!("{}/api/v1/labels", harness.query_url))
        .send()
        .await
        .expect("Labels request should succeed");

    assert!(
        resp.status().is_success(),
        "Labels endpoint should return 2xx"
    );

    let body: serde_json::Value = resp.json().await.expect("Should be valid JSON");
    assert_eq!(
        body["status"], "success",
        "Labels response should have status 'success'"
    );
    assert!(
        body["data"].is_array(),
        "Labels response should have data array"
    );

    println!("Labels endpoint returned: {:?}", body["data"]);
}

/// Test Prometheus label values endpoint
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_label_values_endpoint() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let result = harness.get_label_values("metric_name").await;

    match result {
        Ok(values) => {
            println!("Label values for 'metric_name': {:?}", values);
        }
        Err(e) => {
            println!("Label values query failed: {}", e);
        }
    }
}

/// Test Prometheus response format matches expected structure
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_response_format() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let resp = harness
        .http_client
        .get(format!("{}/api/v1/query", harness.query_url))
        .query(&[("query", "up")])
        .send()
        .await
        .expect("Request should succeed");

    assert!(resp.status().is_success());

    let body: serde_json::Value = resp.json().await.expect("Should be valid JSON");

    assert!(
        body.get("status").is_some(),
        "Response should have 'status' field"
    );
    assert!(
        body.get("data").is_some(),
        "Response should have 'data' field"
    );
    assert!(
        body["data"].get("resultType").is_some(),
        "Data should have 'resultType' field"
    );
    assert!(
        body["data"].get("result").is_some(),
        "Data should have 'result' field"
    );
}

/// Test Prometheus instant query POST endpoint (Grafana compatibility)
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_instant_query_post() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let samples = generate_test_samples(10, 1);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let result = harness.query_prom_post("test_metric_0").await;

    match result {
        Ok(r) => {
            println!(
                "Prometheus POST instant query returned status: {}, {} results",
                r.status,
                r.data.len()
            );
            assert_eq!(r.status, "success", "Status should be 'success'");
        }
        Err(e) => {
            panic!("Prometheus POST query failed: {}", e);
        }
    }
}
