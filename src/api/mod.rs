//! API interfaces for ingestion and querying
//!
//! Supports multiple protocols:
//! - OTLP (OpenTelemetry Protocol) - gRPC and HTTP
//! - Prometheus Remote Write
//! - Arrow Flight (bulk ingestion and queries)
//! - SQL HTTP API
//! - WebSocket/SSE for streaming queries

pub mod grpc;
pub mod ingest;
pub mod query;
mod telemetry;

use axum::Router;
use std::sync::Arc;

/// Combined API server configuration
#[derive(Debug, Clone)]
pub struct ApiServerConfig {
    /// HTTP API port (SQL, Prometheus API)
    pub http_port: u16,
    /// gRPC port (OTLP, Arrow Flight)
    pub grpc_port: u16,
    /// Maximum request body size
    pub max_body_size: usize,
    /// Enable CORS
    pub enable_cors: bool,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            http_port: 8080,
            grpc_port: 4317,
            max_body_size: 16 * 1024 * 1024, // 16MB
            enable_cors: true,
        }
    }
}

/// Build the HTTP API router
pub fn build_http_router(
    ingester: Arc<crate::ingester::Ingester>,
    query_node: Arc<crate::query::QueryNode>,
) -> Router {
    use axum::middleware;
    use axum::routing::{get, post};
    use tower_http::cors::{Any, CorsLayer};

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // Health check
        .route("/health", get(health_check))
        .route("/ready", get(ready_check))

        // SQL API
        .route("/api/v1/sql", post(query::sql_http::execute_sql))
        .route("/api/v1/sql", get(query::sql_http::execute_sql_get))

        // Prometheus API
        .route("/api/v1/query", get(query::prometheus_api::instant_query))
        .route("/api/v1/query", post(query::prometheus_api::instant_query))
        .route("/api/v1/query_range", get(query::prometheus_api::range_query))
        .route("/api/v1/query_range", post(query::prometheus_api::range_query))
        .route("/api/v1/labels", get(query::prometheus_api::labels))
        .route("/api/v1/label/:name/values", get(query::prometheus_api::label_values))

        // Prometheus Remote Write
        .route("/api/v1/write", post(ingest::prometheus::handle_remote_write))

        // Streaming
        .route("/api/v1/stream", get(query::streaming::websocket_handler))

        // State
        .with_state(ApiState {
            ingester,
            query_node,
        })
        .layer(middleware::from_fn(telemetry::http_observability_middleware))
        .layer(cors)
}

/// Shared API state
#[derive(Clone)]
pub struct ApiState {
    pub ingester: Arc<crate::ingester::Ingester>,
    pub query_node: Arc<crate::query::QueryNode>,
}

/// Health check endpoint
async fn health_check() -> &'static str {
    "OK"
}

/// Readiness check endpoint
async fn ready_check() -> &'static str {
    // In production, check dependencies
    "READY"
}
