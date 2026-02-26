//! Rebuild metadata utility
//!
//! This utility rebuilds the time index from existing chunk metadata.
//! Use this after deploying the object-store metadata time-index bug fix.

use cardinalsin::config::ComponentFactory;
use cardinalsin::metadata::{ObjectStoreMetadataClient, ObjectStoreMetadataConfig};
use cardinalsin::telemetry::Telemetry;
use cardinalsin::StorageConfig;

fn provider_env_is_set() -> bool {
    ["CLOUD_PROVIDER", "STORAGE_BACKEND"].iter().any(|key| {
        std::env::var(key)
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = Telemetry::init_for_component("cardinalsin-rebuild-metadata", "info")
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let metadata_container = std::env::var("METADATA_CONTAINER")
        .ok()
        .or_else(|| std::env::var("METADATA_BUCKET").ok());
    let provider_override = if metadata_container.is_some() && !provider_env_is_set() {
        Some("aws")
    } else {
        None
    };

    let storage = ComponentFactory::resolve_storage_config(
        provider_override,
        metadata_container.as_deref(),
        "metadata",
    )?;
    let prefix = std::env::var("METADATA_PREFIX").unwrap_or_else(|_| "metadata/".to_string());

    println!("Rebuilding time index...");
    println!("  Provider: {}", storage.provider.as_str());
    println!("  Bucket: {}", storage.bucket);
    println!("  Prefix: {}", prefix);

    let metadata_storage = StorageConfig {
        provider: storage.provider,
        bucket: storage.bucket.clone(),
        tenant_id: storage.tenant_id.clone(),
    };
    let object_store = ComponentFactory::create_object_store_for(&metadata_storage).await?;

    let config = ObjectStoreMetadataConfig {
        bucket: storage.bucket,
        metadata_prefix: prefix,
        enable_cache: false,
        allow_unsafe_overwrite: std::env::var("S3_METADATA_ALLOW_UNSAFE_OVERWRITE")
            .map(|value| {
                let value = value.trim();
                value == "1" || value.eq_ignore_ascii_case("true")
            })
            .unwrap_or(false),
    };

    let client = ObjectStoreMetadataClient::new(object_store, config);
    client.rebuild_time_index().await?;

    println!("✓ Time index rebuilt successfully");
    Ok(())
}
