//! CardinalSin Query Node Binary
//!
//! Stateless query node that executes queries via DataFusion.

use cardinalsin::api;
use cardinalsin::config::ComponentFactory;
use cardinalsin::ingester::{Ingester, IngesterConfig};
use cardinalsin::query::{QueryConfig, QueryNode};
use cardinalsin::schema::MetricSchema;
use cardinalsin::telemetry::Telemetry;
use cardinalsin::Error;

use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::watch;
use tracing::info;

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

    /// Cloud provider (memory, aws, gcp, azure)
    #[arg(long, env = "CLOUD_PROVIDER")]
    cloud_provider: Option<String>,

    /// Provider bucket
    #[arg(long, env = "STORAGE_BUCKET")]
    storage_bucket: Option<String>,

    /// Deprecated alias for --storage-bucket
    #[arg(long, env = "STORAGE_CONTAINER", hide = true)]
    storage_container: Option<String>,

    /// Deprecated alias for --storage-bucket
    #[arg(long, env = "S3_BUCKET", hide = true)]
    bucket: Option<String>,

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

    let _telemetry = Telemetry::init_for_component("cardinalsin-query", &args.log_level)?;

    info!("Starting CardinalSin Query Node");

    let storage_config = ComponentFactory::resolve_storage_config(
        args.cloud_provider.as_deref(),
        args.storage_bucket
            .as_deref()
            .or(args.storage_container.as_deref())
            .or(args.bucket.as_deref()),
        args.tenant_id.clone(),
    )?;

    let object_store = ComponentFactory::create_object_store_for(&storage_config).await?;

    // Create metadata client from environment
    let metadata =
        ComponentFactory::create_metadata_client_for_storage(object_store.clone(), &storage_config)
            .await?;

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
    let router = api::build_http_router(ingester, query_node.clone());

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
        l1_cache_mb = args.l1_cache_mb,
        l2_cache_mb = args.l2_cache_mb,
        "Query node ready"
    );

    let http_server = async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(wait_for_shutdown(http_shutdown))
            .await
            .map_err(|e| Error::Internal(format!("HTTP server error: {e}")))
    };
    let grpc_server = api::grpc::run_query_grpc_server(grpc_addr, query_node, grpc_shutdown);
    tokio::try_join!(http_server, grpc_server)?;

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

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}
