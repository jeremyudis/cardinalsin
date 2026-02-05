//! Health endpoint tests
//!
//! Validates that all services are running and responding to health checks.

use crate::e2e::E2EHarness;
use std::time::Duration;

/// Test that all services report healthy status
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_all_services_healthy() {
    let harness = E2EHarness::from_env();

    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("All services should be healthy");
}

/// Test that ingester returns READY status
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_ingester_ready() {
    let harness = E2EHarness::from_env();

    // Wait for ingester to be healthy first
    harness
        .wait_ingester_healthy(Duration::from_secs(30))
        .await
        .expect("Ingester should be healthy");

    let resp = harness
        .http_client
        .get(format!("{}/ready", harness.ingester_url))
        .send()
        .await
        .expect("Ready request should succeed");

    assert!(resp.status().is_success(), "Ready endpoint should return 2xx");

    let body = resp.text().await.expect("Should have response body");
    assert_eq!(body, "READY", "Ready endpoint should return 'READY'");
}

/// Test that query node returns READY status
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_query_ready() {
    let harness = E2EHarness::from_env();

    // Wait for query node to be healthy first
    harness
        .wait_query_healthy(Duration::from_secs(30))
        .await
        .expect("Query node should be healthy");

    let resp = harness
        .http_client
        .get(format!("{}/ready", harness.query_url))
        .send()
        .await
        .expect("Ready request should succeed");

    assert!(resp.status().is_success(), "Ready endpoint should return 2xx");

    let body = resp.text().await.expect("Should have response body");
    assert_eq!(body, "READY", "Ready endpoint should return 'READY'");
}

/// Test that ingester health endpoint responds with OK
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_ingester_health_returns_ok() {
    let harness = E2EHarness::from_env();

    harness
        .wait_ingester_healthy(Duration::from_secs(30))
        .await
        .expect("Ingester should be healthy");

    let resp = harness
        .http_client
        .get(format!("{}/health", harness.ingester_url))
        .send()
        .await
        .expect("Health request should succeed");

    assert!(resp.status().is_success(), "Health endpoint should return 2xx");

    let body = resp.text().await.expect("Should have response body");
    assert_eq!(body, "OK", "Health endpoint should return 'OK'");
}

/// Test that query node health endpoint responds with OK
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_query_health_returns_ok() {
    let harness = E2EHarness::from_env();

    harness
        .wait_query_healthy(Duration::from_secs(30))
        .await
        .expect("Query node should be healthy");

    let resp = harness
        .http_client
        .get(format!("{}/health", harness.query_url))
        .send()
        .await
        .expect("Health request should succeed");

    assert!(resp.status().is_success(), "Health endpoint should return 2xx");

    let body = resp.text().await.expect("Should have response body");
    assert_eq!(body, "OK", "Health endpoint should return 'OK'");
}
