//! CardinalSin Ingester Binary
//!
//! Stateless ingester node that receives metrics and writes to object storage.

use cardinalsin::ingester::{Ingester, IngesterConfig};
use cardinalsin::metadata::LocalMetadataClient;
use cardinalsin::schema::MetricSchema;
use cardinalsin::StorageConfig;

use clap::Parser;
use object_store::memory::InMemory;
use std::net::SocketAddr;
use std::sync::Arc;
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

    let subscriber = FmtSubscriber::builder()
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

    // Create object store
    // In production, use object_store::aws::AmazonS3Builder
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    info!("Using in-memory object store (development mode)");

    // Create metadata client
    let metadata: Arc<dyn cardinalsin::metadata::MetadataClient> =
        Arc::new(LocalMetadataClient::new());
    info!("Using local metadata store (development mode)");

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
        storage_config,
        schema,
    ));

    // Start flush timer
    let ingester_timer = ingester.clone();
    tokio::spawn(async move {
        ingester_timer.run_flush_timer().await;
    });

    // Build API router
    // Note: In a real deployment, we'd also start gRPC servers for OTLP

    info!(
        http_port = args.http_port,
        grpc_port = args.grpc_port,
        "Ingester ready"
    );

    // Wait for shutdown signal
    shutdown_signal().await;

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
