//! CardinalSin Ingester Binary
//!
//! Stateless ingester node that receives metrics and writes to object storage.

use cardinalsin::api;
use cardinalsin::config::ComponentFactory;
use cardinalsin::ingester::{Ingester, IngesterConfig, WalConfig, WalSyncMode};
use cardinalsin::query::{QueryConfig, QueryNode};
use cardinalsin::schema::MetricSchema;
use cardinalsin::{Error, StorageConfig};

use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::watch;
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

    /// Enable WAL for crash durability (default: true)
    #[arg(long, env = "WAL_ENABLED", default_value = "true")]
    wal_enabled: bool,

    /// WAL directory
    #[arg(long, env = "WAL_DIR", default_value = "/var/lib/cardinalsin/wal")]
    wal_dir: String,

    /// WAL sync mode: every_write, interval_100ms, interval_1s, on_rotation, none
    #[arg(long, env = "WAL_SYNC_MODE", default_value = "interval_100ms")]
    wal_sync_mode: String,

    /// WAL max segment size in bytes (default: 64MB)
    #[arg(long, env = "WAL_MAX_SEGMENT_SIZE", default_value = "67108864")]
    wal_max_segment_size: usize,

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

    // Parse WAL sync mode
    let wal_sync_mode = WalSyncMode::from_str(&args.wal_sync_mode)
        .map_err(|e| format!("invalid WAL_SYNC_MODE: {e}"))?;

    // Create ingester
    let ingester_config = IngesterConfig {
        flush_interval: std::time::Duration::from_secs(args.flush_interval_secs),
        wal: WalConfig {
            wal_dir: std::path::PathBuf::from(&args.wal_dir),
            max_segment_size: args.wal_max_segment_size,
            sync_mode: wal_sync_mode,
            enabled: args.wal_enabled,
        },
        ..Default::default()
    };

    info!(
        wal_enabled = ingester_config.wal.enabled,
        wal_dir = %args.wal_dir,
        wal_sync_mode = %args.wal_sync_mode,
        wal_max_segment_size = args.wal_max_segment_size,
        "WAL configuration"
    );

    let schema = MetricSchema::default_metrics();

    let mut ingester = Ingester::new(
        ingester_config,
        object_store,
        metadata,
        storage_config.clone(),
        schema,
    );

    // Initialize WAL and recover any unflushed entries
    ingester.ensure_wal().await?;

    let ingester = Arc::new(ingester);

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
            ComponentFactory::create_metadata_client(
                ComponentFactory::create_object_store().await?,
            )
            .await?,
            storage_config.clone(),
        )
        .await?,
    );

    // Build HTTP router
    let router = api::build_http_router(ingester.clone(), query_node);

    // Start HTTP server
    let addr = SocketAddr::from(([0, 0, 0, 0], args.http_port));
    let grpc_addr = SocketAddr::from(([0, 0, 0, 0], args.grpc_port));
    let listener = TcpListener::bind(addr).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let http_shutdown = shutdown_rx.clone();
    let grpc_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    info!(
        http_port = args.http_port,
        grpc_port = args.grpc_port,
        "Ingester ready"
    );

    let http_server = async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(wait_for_shutdown(http_shutdown))
            .await
            .map_err(|e| Error::Internal(format!("HTTP server error: {e}")))
    };
    let grpc_server = api::grpc::run_ingester_grpc_server(grpc_addr, ingester, grpc_shutdown);
    tokio::try_join!(http_server, grpc_server)?;

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

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}
