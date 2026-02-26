//! Rebuild metadata utility
//!
//! This utility rebuilds the time index from existing chunk metadata.
//! Use this after deploying the object-store metadata time-index bug fix.

use cardinalsin::config::ComponentFactory;
use cardinalsin::metadata::{ObjectStoreMetadataClient, ObjectStoreMetadataConfig};
use cardinalsin::telemetry::Telemetry;
use cardinalsin::StorageConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = Telemetry::init_for_component("cardinalsin-rebuild-metadata", "info")
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let storage = ComponentFactory::resolve_storage_config(None, None, "metadata")?;
    let prefix = std::env::var("METADATA_PREFIX").unwrap_or_else(|_| "metadata/".to_string());

    let metadata_container = std::env::var("METADATA_CONTAINER")
        .ok()
        .or_else(|| std::env::var("METADATA_BUCKET").ok())
        .unwrap_or_else(|| storage.container.clone());

    println!("Rebuilding time index...");
    println!("  Provider: {}", storage.provider.as_str());
    println!("  Container: {}", metadata_container);
    println!("  Prefix: {}", prefix);

    let metadata_storage = StorageConfig {
        provider: storage.provider,
        container: metadata_container.clone(),
        tenant_id: storage.tenant_id.clone(),
    };
    let object_store = ComponentFactory::create_object_store_for(&metadata_storage).await?;

    let config = ObjectStoreMetadataConfig {
        bucket: metadata_container,
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
