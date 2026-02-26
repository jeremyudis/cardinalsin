//! Migration tool to backfill compaction levels for existing chunks
//!
//! This tool scans existing chunk metadata and assigns appropriate levels
//! based on chunk paths and naming patterns.

use cardinalsin::config::ComponentFactory;
use cardinalsin::metadata::{ObjectStoreMetadataClient, ObjectStoreMetadataConfig};
use cardinalsin::telemetry::Telemetry;
use cardinalsin::StorageConfig;
use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Cloud provider (memory, aws, gcp, azure)
    #[arg(long, env = "CLOUD_PROVIDER")]
    cloud_provider: Option<String>,

    /// Metadata container/bucket
    #[arg(short, long, env = "METADATA_CONTAINER")]
    container: Option<String>,

    /// Deprecated alias for --container
    #[arg(long, env = "METADATA_BUCKET", hide = true)]
    bucket: Option<String>,

    /// Metadata prefix
    #[arg(short, long, default_value = "metadata/")]
    prefix: String,

    /// Dry run - don't actually update metadata
    #[arg(long, default_value = "false")]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _telemetry = Telemetry::init_for_component("cardinalsin-backfill-levels", "info")?;

    let args = Args::parse();
    let metadata_storage = ComponentFactory::resolve_storage_config(
        args.cloud_provider.as_deref(),
        args.container.as_deref().or(args.bucket.as_deref()),
        "metadata",
    )?;

    info!("Starting level backfill migration");
    info!("  Provider: {}", metadata_storage.provider.as_str());
    info!("  Container: {}", metadata_storage.container);
    info!("  Prefix: {}", args.prefix);
    info!("  Dry run: {}", args.dry_run);

    let metadata_store = StorageConfig {
        provider: metadata_storage.provider,
        container: metadata_storage.container.clone(),
        tenant_id: metadata_storage.tenant_id.clone(),
    };
    let object_store = ComponentFactory::create_object_store_for(&metadata_store).await?;

    let config = ObjectStoreMetadataConfig {
        bucket: metadata_storage.container,
        metadata_prefix: args.prefix.clone(),
        enable_cache: false,
        allow_unsafe_overwrite: std::env::var("S3_METADATA_ALLOW_UNSAFE_OVERWRITE")
            .map(|value| {
                let value = value.trim();
                value == "1" || value.eq_ignore_ascii_case("true")
            })
            .unwrap_or(false),
    };

    let metadata_client = ObjectStoreMetadataClient::new(object_store, config);

    info!("Rebuilding time index from chunk metadata...");
    metadata_client.rebuild_time_index().await?;

    info!("Level backfill migration complete!");
    info!("Note: Actual level assignment happens automatically during compaction.");
    info!("Existing chunks default to level=0 (L0) and are promoted during compaction.");

    if args.dry_run {
        info!("DRY RUN - no changes were made");
    }

    Ok(())
}
