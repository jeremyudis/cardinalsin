//! CardinalSin Query Node Binary
//!
//! Stateless query node that executes queries via DataFusion.

use cardinalsin::api;
use cardinalsin::ingester::{Ingester, IngesterConfig};
use cardinalsin::metadata::LocalMetadataClient;
use cardinalsin::query::{QueryNode, QueryConfig};
use cardinalsin::schema::MetricSchema;
use cardinalsin::StorageConfig;

use axum::Router;
use clap::Parser;
use object_store::memory::InMemory;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// CardinalSin Query Node
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// HTTP API port
    #[arg(long, default_value = "8080")]
    http_port: u16,

    /// gRPC port (Flight SQL)
    #[arg(long, default_value = "8815")]
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

    /// L1 cache size in MB
    #[arg(long, default_value = "1024")]
    l1_cache_mb: usize,

    /// L2 cache size in MB
    #[arg(long, default_value = "10240")]
    l2_cache_mb: usize,

    /// L2 cache directory
    #[arg(long, env = "CACHE_DIR")]
    cache_dir: Option<String>,

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

    FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(true)
        .with_thread_ids(true)
        .json()
        .init();

    info!("Starting CardinalSin Query Node");

    // Create storage config
    let storage_config = StorageConfig {
        bucket: args.bucket.clone(),
        region: args.region.clone(),
        endpoint: args.endpoint.clone(),
        tenant_id: args.tenant_id.clone(),
    };

    // Create object store
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    info!("Using in-memory object store (development mode)");

    // Create metadata client
    let metadata: Arc<dyn cardinalsin::metadata::MetadataClient> =
        Arc::new(LocalMetadataClient::new());

    // Create query config
    let query_config = QueryConfig {
        l1_cache_size: args.l1_cache_mb * 1024 * 1024,
        l2_cache_size: args.l2_cache_mb * 1024 * 1024,
        l2_cache_dir: args.cache_dir.clone(),
        ..Default::default()
    };

    // Create query node
    let query_node = Arc::new(
        QueryNode::new(
            query_config,
            object_store.clone(),
            metadata.clone(),
            storage_config.clone(),
        )
        .await?,
    );

    // Create a dummy ingester for the API (in production, query nodes don't ingest)
    let ingester = Arc::new(Ingester::new(
        IngesterConfig::default(),
        object_store,
        metadata,
        storage_config,
        MetricSchema::default_metrics(),
    ));

    // Build HTTP router
    let router = api::build_http_router(ingester, query_node);

    // Start HTTP server
    let addr = SocketAddr::from(([0, 0, 0, 0], args.http_port));
    let listener = TcpListener::bind(addr).await?;

    info!(
        http_port = args.http_port,
        grpc_port = args.grpc_port,
        l1_cache_mb = args.l1_cache_mb,
        l2_cache_mb = args.l2_cache_mb,
        "Query node ready"
    );

    // Start server with graceful shutdown
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Query node shutting down");

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
