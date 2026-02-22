//! CardinalSin Compactor Binary
//!
//! Background service that merges and optimizes Parquet files.

use cardinalsin::compactor::{Compactor, CompactorConfig};
use cardinalsin::config::ComponentFactory;
use cardinalsin::sharding::{HotShardConfig, ShardMonitor};
use cardinalsin::telemetry::Telemetry;
use cardinalsin::StorageConfig;

use clap::Parser;
use std::sync::Arc;
use tokio::signal;
use tracing::info;

/// CardinalSin Compactor
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
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

    /// L0 merge threshold (number of files)
    #[arg(long, default_value = "15")]
    l0_threshold: usize,

    /// Check interval in seconds
    #[arg(long, default_value = "60")]
    check_interval_secs: u64,

    /// Retention period in days
    #[arg(long, default_value = "90")]
    retention_days: u32,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let _telemetry = Telemetry::init_for_component("cardinalsin-compactor", &args.log_level)?;

    info!("Starting CardinalSin Compactor");

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

    // Create compactor config
    let compactor_config = CompactorConfig {
        l0_merge_threshold: args.l0_threshold,
        check_interval: std::time::Duration::from_secs(args.check_interval_secs),
        retention_days: args.retention_days,
        ..Default::default()
    };

    // Create shard monitor for hot shard detection
    let shard_monitor = Arc::new(ShardMonitor::new(HotShardConfig::default()));

    // Create compactor
    let compactor = Compactor::new(
        compactor_config,
        object_store,
        metadata,
        storage_config,
        shard_monitor,
    );

    info!(
        l0_threshold = args.l0_threshold,
        check_interval_secs = args.check_interval_secs,
        retention_days = args.retention_days,
        "Compactor ready"
    );

    // Run compaction loop with graceful shutdown
    tokio::select! {
        _ = compactor.run() => {},
        _ = shutdown_signal() => {},
    }

    info!("Compactor shutting down");

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
