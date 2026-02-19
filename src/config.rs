//! Component factory for environment-based configuration
//!
//! This module provides factory methods to create object stores and metadata clients
//! based on environment variables, enabling easy switching between development and
//! production configurations.

use crate::metadata::{LocalMetadataClient, MetadataClient, S3MetadataClient, S3MetadataConfig};
use crate::Result;
use object_store::{aws::AmazonS3Builder, memory::InMemory, ObjectStore};
use std::sync::Arc;
use tracing::info;

pub struct ComponentFactory;

impl ComponentFactory {
    /// Create object store from environment
    ///
    /// Environment variables:
    /// - STORAGE_BACKEND: "memory" (default) or "s3"
    /// - S3_BUCKET: S3 bucket name (required for s3)
    /// - S3_REGION: S3 region (default: us-east-1)
    /// - S3_ENDPOINT: Custom S3 endpoint (optional, for MinIO)
    /// - AWS_ACCESS_KEY_ID: AWS credentials (optional, uses IAM role if not set)
    /// - AWS_SECRET_ACCESS_KEY: AWS credentials (optional)
    pub async fn create_object_store() -> Result<Arc<dyn ObjectStore>> {
        let backend = std::env::var("STORAGE_BACKEND").unwrap_or_else(|_| "memory".to_string());

        match backend.as_str() {
            "memory" => {
                info!("Using in-memory object store (development mode)");
                Ok(Arc::new(InMemory::new()))
            }
            "s3" => {
                let bucket = std::env::var("S3_BUCKET").map_err(|_| {
                    crate::Error::Config("S3_BUCKET required when STORAGE_BACKEND=s3".to_string())
                })?;
                let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

                info!(
                    "Using S3 object store: bucket={}, region={}",
                    bucket, region
                );

                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(&bucket)
                    .with_region(&region);

                // Support custom endpoints (MinIO, LocalStack)
                if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
                    info!("Using custom S3 endpoint: {}", endpoint);
                    builder = builder.with_endpoint(&endpoint).with_allow_http(true);
                }

                // Use explicit credentials if provided, otherwise use IAM role
                if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID") {
                    builder = builder.with_access_key_id(&key);
                }
                if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                    builder = builder.with_secret_access_key(&secret);
                }

                Ok(Arc::new(builder.build()?))
            }
            _ => Err(crate::Error::Config(format!(
                "Unknown STORAGE_BACKEND: {}. Use 'memory' or 's3'",
                backend
            ))),
        }
    }

    /// Create metadata client from environment
    ///
    /// Environment variables:
    /// - METADATA_BACKEND: "local" (default) or "s3"
    /// - METADATA_BUCKET: S3 bucket for metadata (defaults to S3_BUCKET)
    /// - METADATA_PREFIX: S3 prefix for metadata (default: metadata/)
    pub async fn create_metadata_client(
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn MetadataClient>> {
        let backend = std::env::var("METADATA_BACKEND").unwrap_or_else(|_| "local".to_string());

        match backend.as_str() {
            "local" => {
                info!("Using LocalMetadataClient (development mode)");
                Ok(Arc::new(LocalMetadataClient::new()))
            }
            "s3" => {
                let bucket = std::env::var("METADATA_BUCKET")
                    .or_else(|_| std::env::var("S3_BUCKET"))
                    .map_err(|_| {
                        crate::Error::Config(
                            "METADATA_BUCKET or S3_BUCKET required when METADATA_BACKEND=s3"
                                .to_string(),
                        )
                    })?;
                let prefix =
                    std::env::var("METADATA_PREFIX").unwrap_or_else(|_| "metadata/".to_string());

                info!(
                    "Using S3MetadataClient: bucket={}, prefix={}",
                    bucket, prefix
                );

                let config = S3MetadataConfig {
                    bucket,
                    metadata_prefix: prefix,
                    enable_cache: true,
                    allow_unsafe_overwrite: std::env::var("S3_METADATA_ALLOW_UNSAFE_OVERWRITE")
                        .map(|value| {
                            let value = value.trim();
                            value == "1" || value.eq_ignore_ascii_case("true")
                        })
                        .unwrap_or(false),
                };

                Ok(Arc::new(S3MetadataClient::new(object_store, config)))
            }
            _ => Err(crate::Error::Config(format!(
                "Unknown METADATA_BACKEND: {}. Use 'local' or 's3'",
                backend
            ))),
        }
    }
}
