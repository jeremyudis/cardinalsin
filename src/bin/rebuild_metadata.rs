//! Rebuild metadata utility
//!
//! This utility rebuilds the time index from existing chunk metadata.
//! Use this after deploying the S3MetadataClient time index bug fix.

use cardinalsin::metadata::{S3MetadataClient, S3MetadataConfig};
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let bucket = std::env::var("METADATA_BUCKET")
        .or_else(|_| std::env::var("S3_BUCKET"))
        .expect("METADATA_BUCKET or S3_BUCKET environment variable required");

    let prefix = std::env::var("METADATA_PREFIX")
        .unwrap_or_else(|_| "metadata/".to_string());

    println!("Rebuilding time index...");
    println!("  Bucket: {}", bucket);
    println!("  Prefix: {}", prefix);

    let config = S3MetadataConfig {
        bucket: bucket.clone(),
        metadata_prefix: prefix,
        enable_cache: false,
        allow_unsafe_overwrite: std::env::var("S3_METADATA_ALLOW_UNSAFE_OVERWRITE")
            .map(|value| {
                let value = value.trim();
                value == "1" || value.eq_ignore_ascii_case("true")
            })
            .unwrap_or(false),
    };

    // Build S3 client
    let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);

    if let Ok(region) = std::env::var("S3_REGION") {
        builder = builder.with_region(&region);
    }

    if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
        builder = builder.with_endpoint(&endpoint).with_allow_http(true);
    }

    if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID") {
        builder = builder.with_access_key_id(&key);
    }

    if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        builder = builder.with_secret_access_key(&secret);
    }

    let object_store = Arc::new(builder.build()?);
    let client = S3MetadataClient::new(object_store, config);

    client.rebuild_time_index().await?;
    println!("✓ Time index rebuilt successfully");

    Ok(())
}
