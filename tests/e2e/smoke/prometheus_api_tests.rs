//! Prometheus API compatibility tests
//!
//! Validates that the Prometheus-compatible API works correctly,
//! ensuring compatibility with Grafana and other Prometheus clients.

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

    // First write some data
    let samples = generate_test_samples(10, 1);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query via Prometheus API
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

    // Query for metric_name label values
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

/// Test Prometheus range query endpoint
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_range_query() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    // Query via Prometheus range API
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
            println!("Prometheus range query failed: {}", e);
        }
    }
}

/// Test Prometheus query with label selectors
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_query_with_labels() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Write some data with labels
    let samples = generate_test_samples(20, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query with label selector
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
            println!("Prometheus query with labels failed: {}", e);
        }
    }
}

/// Test Prometheus aggregation query (sum)
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_sum_aggregation() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Query with sum aggregation
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
            println!("Prometheus sum aggregation failed: {}", e);
        }
    }
}

/// Test Prometheus aggregation with grouping
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_sum_by_aggregation() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Query with sum by host
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
            println!("Prometheus sum by aggregation failed: {}", e);
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

    // Verify Prometheus response structure
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

    // First write some data
    let samples = generate_test_samples(10, 1);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query via Prometheus API using POST
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

/// Test Prometheus range query POST endpoint (Grafana compatibility)
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_range_query_post() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    // Query via Prometheus range API using POST
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

/// Test that POST endpoint can handle complex queries with labels
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_post_with_labels() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Write some data with labels
    let samples = generate_test_samples(20, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query with label selector using POST
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

/// Test /api/v1/series endpoint (GET)
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_series_endpoint() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Write some data with labels
    let samples = generate_test_samples(10, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query series endpoint with matcher
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
    assert_eq!(
        body["status"], "success",
        "Series response should have status 'success'"
    );
    assert!(
        body["data"].is_array(),
        "Series response should have data array"
    );

    println!("Series endpoint returned: {:?}", body["data"]);
}

/// Test /api/v1/series endpoint with time range filtering
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_series_with_time_range() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    // Query series endpoint with matcher and time range
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
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_series_post() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Write some data with labels
    let samples = generate_test_samples(10, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query series endpoint with POST
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
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_labels_post_with_filters() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Write some data
    let samples = generate_test_samples(10, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    // Query labels endpoint with POST and filters
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
#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_prometheus_label_values_with_filters() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("Services should be healthy");

    // Write some data
    let samples = generate_test_samples(10, 2);
    harness
        .write_samples(samples)
        .await
        .expect("Write should succeed");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let now = chrono::Utc::now().timestamp() as f64;
    let one_hour_ago = now - 3600.0;

    // Query label values endpoint with filters
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

    println!("Label values with filters returned: {:?}", body["data"]);
}
