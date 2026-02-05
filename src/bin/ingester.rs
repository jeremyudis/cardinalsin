//! CardinalSin Ingester Binary
//!
//! Stateless ingester node that receives metrics and writes to object storage.

use cardinalsin::api;
use cardinalsin::config::ComponentFactory;
use cardinalsin::ingester::{Ingester, IngesterConfig};
use cardinalsin::query::{QueryConfig, QueryNode};
use cardinalsin::schema::MetricSchema;
use cardinalsin::StorageConfig;

use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// CardinalSin Ingester
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// HTTP API port
    #[arg(long, default_value = "8080")]
    http_port: u16,

    /// gRPC port (OTLP)
    #[arg(long, default_value = "4317")]
    grpc_port: u16,

    /// S3 bucket name
    #[arg(long, env = "S3_BUCKET", default_value = "cardinalsin-data")]
    bucket: String,

    /// S3 region
    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    region: String,

    /// S3 endpoint (for MinIO)
    #[arg(long, env = "S3_ENDPOINT")]
    endpoint: Option<String>,

    /// Tenant ID
    #[arg(long, env = "TENANT_ID", default_value = "default")]
    tenant_id: String,

    /// Flush interval in seconds
    #[arg(long, default_value = "300")]
    flush_interval_secs: u64,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let _subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(true)
        .with_thread_ids(true)
        .json()
        .init();

    info!("Starting CardinalSin Ingester");

    // Create storage config
    let storage_config = StorageConfig {
        bucket: args.bucket.clone(),
        region: args.region.clone(),
        endpoint: args.endpoint.clone(),
        tenant_id: args.tenant_id.clone(),
    };

    // Create object store from environment
    let object_store = ComponentFactory::create_object_store().await?;

    // Create metadata client from environment
    let metadata = ComponentFactory::create_metadata_client(object_store.clone()).await?;

    // Create ingester
    let ingester_config = IngesterConfig {
        flush_interval: std::time::Duration::from_secs(args.flush_interval_secs),
        ..Default::default()
    };

    let schema = MetricSchema::default_metrics();

    let ingester = Arc::new(Ingester::new(
        ingester_config,
        object_store,
        metadata,
        storage_config.clone(),
        schema,
    ));

    // Start flush timer
    let ingester_timer = ingester.clone();
    tokio::spawn(async move {
        ingester_timer.run_flush_timer().await;
    });

    // Create a minimal query node for the API (ingester doesn't serve queries, but API needs it)
    let query_node = Arc::new(
        QueryNode::new(
            QueryConfig::default(),
            ComponentFactory::create_object_store().await?,
            ComponentFactory::create_metadata_client(ComponentFactory::create_object_store().await?).await?,
            storage_config.clone(),
        )
        .await?,
    );

    // Build HTTP router
    let router = api::build_http_router(ingester, query_node);

    // Start HTTP server
    let addr = SocketAddr::from(([0, 0, 0, 0], args.http_port));
    let listener = TcpListener::bind(addr).await?;

    info!(
        http_port = args.http_port,
        grpc_port = args.grpc_port,
        "Ingester ready"
    );

    // Start server with graceful shutdown
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Ingester shutting down");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
