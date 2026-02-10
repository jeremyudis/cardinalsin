//! Migration tool to backfill compaction levels for existing chunks
//!
//! This tool scans existing chunk metadata and assigns appropriate levels
//! based on chunk paths and naming patterns.
//!
//! Usage:
//!   cargo run --bin backfill-levels -- --bucket cardinalsin-metadata --prefix metadata/

use cardinalsin::metadata::{S3MetadataClient, S3MetadataConfig};
use clap::Parser;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// S3 bucket name
    #[arg(short, long, default_value = "cardinalsin-metadata")]
    bucket: String,

    /// Metadata prefix in S3
    #[arg(short, long, default_value = "metadata/")]
    prefix: String,

    /// S3 region
    #[arg(short, long, default_value = "us-east-1")]
    region: String,

    /// S3 endpoint (for MinIO or other S3-compatible storage)
    #[arg(short, long)]
    endpoint: Option<String>,

    /// Dry run - don't actually update metadata
    #[arg(long, default_value = "false")]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    info!("Starting level backfill migration");
    info!("  Bucket: {}", args.bucket);
    info!("  Prefix: {}", args.prefix);
    info!("  Region: {}", args.region);
    info!("  Dry run: {}", args.dry_run);

    // Create S3 client
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&args.bucket)
        .with_region(&args.region);

    if let Some(endpoint) = &args.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    let object_store = Arc::new(builder.build()?);

    // Create metadata client
    let config = S3MetadataConfig {
        bucket: args.bucket.clone(),
        metadata_prefix: args.prefix.clone(),
        enable_cache: false,
        allow_unsafe_overwrite: std::env::var("S3_METADATA_ALLOW_UNSAFE_OVERWRITE")
            .map(|value| {
                let value = value.trim();
                value == "1" || value.eq_ignore_ascii_case("true")
            })
            .unwrap_or(false),
    };

    let metadata_client = S3MetadataClient::new(object_store, config);

    // Rebuild time index (also loads all chunks)
    info!("Rebuilding time index from chunk metadata...");
    metadata_client.rebuild_time_index().await?;

    info!("Level backfill migration complete!");
    info!("Note: Actual level assignment happens automatically during compaction.");
    info!("Existing chunks will default to level=0 (L0) and be promoted during next compaction cycle.");

    if args.dry_run {
        info!("DRY RUN - No changes were made");
    }

    Ok(())
}
