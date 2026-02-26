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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};

    const TEST_ENV_KEYS: &[&str] = &[
        "STORAGE_BACKEND",
        "S3_BUCKET",
        "S3_REGION",
        "S3_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "METADATA_BACKEND",
        "METADATA_BUCKET",
        "METADATA_PREFIX",
        "S3_METADATA_ALLOW_UNSAFE_OVERWRITE",
    ];

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env<F>(overrides: &[(&str, Option<&str>)], f: F)
    where
        F: FnOnce(),
    {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let mut saved: Vec<(&str, Option<OsString>)> = Vec::new();

        for key in TEST_ENV_KEYS {
            saved.push((key, std::env::var_os(key)));
            // SAFETY: tests serialize environment mutation with a global mutex.
            unsafe { std::env::remove_var(key) };
        }

        for (key, value) in overrides {
            match value {
                Some(v) => {
                    // SAFETY: tests serialize environment mutation with a global mutex.
                    unsafe { std::env::set_var(key, v) };
                }
                None => {
                    // SAFETY: tests serialize environment mutation with a global mutex.
                    unsafe { std::env::remove_var(key) };
                }
            }
        }

        f();

        for (key, value) in saved {
            match value {
                Some(v) => {
                    // SAFETY: tests serialize environment mutation with a global mutex.
                    unsafe { std::env::set_var(key, v) };
                }
                None => {
                    // SAFETY: tests serialize environment mutation with a global mutex.
                    unsafe { std::env::remove_var(key) };
                }
            }
        }
    }

    #[test]
    fn create_object_store_defaults_to_memory() {
        with_env(&[], || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let result = rt.block_on(ComponentFactory::create_object_store());
            assert!(result.is_ok(), "default memory backend should work");
        });
    }

    #[test]
    fn create_object_store_rejects_unknown_backend() {
        with_env(&[("STORAGE_BACKEND", Some("gcs"))], || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let err = rt
                .block_on(ComponentFactory::create_object_store())
                .expect_err("unknown backend should fail");
            assert!(
                err.to_string().contains("Unknown STORAGE_BACKEND"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn create_object_store_s3_requires_bucket() {
        with_env(&[("STORAGE_BACKEND", Some("s3"))], || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let err = rt
                .block_on(ComponentFactory::create_object_store())
                .expect_err("s3 backend without bucket should fail");
            assert!(
                err.to_string().contains("S3_BUCKET required"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn create_object_store_s3_accepts_bucket() {
        with_env(
            &[
                ("STORAGE_BACKEND", Some("s3")),
                ("S3_BUCKET", Some("test-bucket")),
                ("S3_REGION", Some("us-east-1")),
                ("S3_ENDPOINT", Some("http://localhost:9000")),
                ("AWS_ACCESS_KEY_ID", Some("minioadmin")),
                ("AWS_SECRET_ACCESS_KEY", Some("minioadmin")),
            ],
            || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let result = rt.block_on(ComponentFactory::create_object_store());
                assert!(result.is_ok(), "configured s3 backend should build");
            },
        );
    }

    #[test]
    fn create_metadata_client_defaults_to_local() {
        with_env(&[], || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let result = rt.block_on(ComponentFactory::create_metadata_client(object_store));
            assert!(result.is_ok(), "default metadata backend should be local");
        });
    }

    #[test]
    fn create_metadata_client_s3_requires_bucket_env() {
        with_env(&[("METADATA_BACKEND", Some("s3"))], || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            match rt.block_on(ComponentFactory::create_metadata_client(object_store)) {
                Ok(_) => panic!("s3 metadata backend without bucket should fail"),
                Err(err) => assert!(
                    err.to_string()
                        .contains("METADATA_BUCKET or S3_BUCKET required"),
                    "unexpected error: {err}"
                ),
            }
        });
    }

    #[test]
    fn create_metadata_client_s3_uses_s3_bucket_fallback() {
        with_env(
            &[
                ("METADATA_BACKEND", Some("s3")),
                ("S3_BUCKET", Some("metadata-bucket")),
            ],
            || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let result = rt.block_on(ComponentFactory::create_metadata_client(object_store));
                assert!(result.is_ok(), "s3 metadata should use S3_BUCKET fallback");
            },
        );
    }

    #[test]
    fn create_metadata_client_rejects_unknown_backend() {
        with_env(&[("METADATA_BACKEND", Some("redis"))], || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            match rt.block_on(ComponentFactory::create_metadata_client(object_store)) {
                Ok(_) => panic!("unknown metadata backend should fail"),
                Err(err) => assert!(
                    err.to_string().contains("Unknown METADATA_BACKEND"),
                    "unexpected error: {err}"
                ),
            }
        });
    }
}
