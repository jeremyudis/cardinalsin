//! Component factory for environment-based configuration
//!
//! This module provides factory methods to create object stores and metadata clients
//! based on environment variables, enabling easy switching between development and
//! production configurations.

use crate::metadata::{
    LocalMetadataClient, MetadataClient, ObjectStoreMetadataClient, ObjectStoreMetadataConfig,
};
use crate::{CloudProvider, Result, StorageConfig};
use object_store::{
    aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
    memory::InMemory, ObjectStore,
};
use std::sync::Arc;
use tracing::{info, warn};

pub struct ComponentFactory;

impl ComponentFactory {
    /// Resolve a provider-neutral storage config from overrides and environment.
    pub fn resolve_storage_config(
        provider_override: Option<&str>,
        container_override: Option<&str>,
        tenant_id: impl Into<String>,
    ) -> Result<StorageConfig> {
        let provider = Self::resolve_cloud_provider(provider_override)?;
        let container = Self::resolve_storage_container(provider, container_override)?;

        Ok(StorageConfig {
            provider,
            container,
            tenant_id: tenant_id.into(),
        })
    }

    /// Create object store from environment.
    ///
    /// Environment variables:
    /// - CLOUD_PROVIDER: "memory" (default), "aws", "gcp", or "azure"
    /// - STORAGE_CONTAINER: provider container/bucket name (required for aws/gcp/azure)
    /// - STORAGE_BACKEND: deprecated alias for CLOUD_PROVIDER
    pub async fn create_object_store() -> Result<Arc<dyn ObjectStore>> {
        let storage = Self::resolve_storage_config(None, None, "default")?;
        Self::create_object_store_for(&storage).await
    }

    /// Create object store from explicit storage configuration.
    pub async fn create_object_store_for(storage: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
        Self::warn_ignored_provider_envs(storage.provider);

        match storage.provider {
            CloudProvider::Memory => {
                info!("Using in-memory object store (development mode)");
                Ok(Arc::new(InMemory::new()))
            }
            CloudProvider::Aws => {
                let region = Self::read_env("S3_REGION").unwrap_or_else(|| "us-east-1".to_string());

                info!(
                    provider = "aws",
                    container = %storage.container,
                    region = %region,
                    "Using object store"
                );

                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(&storage.container)
                    .with_region(&region);

                if let Some(endpoint) = Self::read_env("S3_ENDPOINT") {
                    info!(endpoint = %endpoint, "Using custom S3 endpoint");
                    builder = builder.with_endpoint(&endpoint).with_allow_http(true);
                }

                if let Some(key) = Self::read_env("AWS_ACCESS_KEY_ID") {
                    builder = builder.with_access_key_id(&key);
                }
                if let Some(secret) = Self::read_env("AWS_SECRET_ACCESS_KEY") {
                    builder = builder.with_secret_access_key(&secret);
                }

                Ok(Arc::new(builder.build()?))
            }
            CloudProvider::Gcp => {
                info!(provider = "gcp", container = %storage.container, "Using object store");
                let store = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&storage.container)
                    .build()?;
                Ok(Arc::new(store))
            }
            CloudProvider::Azure => {
                info!(provider = "azure", container = %storage.container, "Using object store");
                let store = MicrosoftAzureBuilder::from_env()
                    .with_container_name(&storage.container)
                    .build()?;
                Ok(Arc::new(store))
            }
        }
    }

    /// Create metadata client from environment.
    ///
    /// Environment variables:
    /// - METADATA_BACKEND: "local" (default) or "object_store"
    /// - METADATA_BACKEND=s3: deprecated alias for "object_store"
    /// - METADATA_CONTAINER: metadata container/bucket (optional, defaults to STORAGE_CONTAINER)
    /// - METADATA_BUCKET: deprecated alias for METADATA_CONTAINER
    /// - METADATA_PREFIX: object-store prefix for metadata (default: metadata/)
    pub async fn create_metadata_client(
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn MetadataClient>> {
        let storage = Self::resolve_storage_config(None, None, "default")?;
        Self::create_metadata_client_for_storage(object_store, &storage).await
    }

    /// Create metadata client using an explicit storage configuration.
    pub async fn create_metadata_client_for_storage(
        object_store: Arc<dyn ObjectStore>,
        storage: &StorageConfig,
    ) -> Result<Arc<dyn MetadataClient>> {
        let backend_raw = Self::read_env("METADATA_BACKEND").unwrap_or_else(|| "local".to_string());
        let backend = backend_raw.trim().to_ascii_lowercase();

        match backend.as_str() {
            "local" => {
                info!("Using LocalMetadataClient (development mode)");
                Ok(Arc::new(LocalMetadataClient::new()))
            }
            "s3" => {
                warn!("METADATA_BACKEND=s3 is deprecated; use METADATA_BACKEND=object_store");
                Self::create_object_store_metadata_client(object_store, storage).await
            }
            "object_store" => {
                Self::create_object_store_metadata_client(object_store, storage).await
            }
            _ => Err(crate::Error::Config(format!(
                "Unknown METADATA_BACKEND: {}. Use 'local' or 'object_store'",
                backend_raw
            ))),
        }
    }

    async fn create_object_store_metadata_client(
        object_store: Arc<dyn ObjectStore>,
        storage: &StorageConfig,
    ) -> Result<Arc<dyn MetadataClient>> {
        let provider = storage.provider;
        let storage_container = storage.container.clone();

        let metadata_container = match (
            Self::read_env("METADATA_CONTAINER"),
            Self::read_env("METADATA_BUCKET"),
        ) {
            (Some(container), Some(bucket)) => {
                if container != bucket {
                    warn!(
                        metadata_container = %container,
                        metadata_bucket = %bucket,
                        "Ignoring METADATA_BUCKET because METADATA_CONTAINER is set"
                    );
                }
                container
            }
            (Some(container), None) => container,
            (None, Some(bucket)) => {
                warn!("METADATA_BUCKET is deprecated; use METADATA_CONTAINER");
                bucket
            }
            (None, None) => storage_container.clone(),
        };

        let metadata_store = if metadata_container == storage_container {
            object_store
        } else {
            warn!(
                storage_container = %storage_container,
                metadata_container = %metadata_container,
                "Creating dedicated object store for metadata container"
            );
            let metadata_storage = StorageConfig {
                provider,
                container: metadata_container.clone(),
                tenant_id: "metadata".to_string(),
            };
            Self::create_object_store_for(&metadata_storage).await?
        };

        let prefix = Self::read_env("METADATA_PREFIX").unwrap_or_else(|| "metadata/".to_string());

        info!(
            backend = "object_store",
            provider = %provider.as_str(),
            container = %metadata_container,
            prefix = %prefix,
            "Using metadata client"
        );

        let config = ObjectStoreMetadataConfig {
            bucket: metadata_container,
            metadata_prefix: prefix,
            enable_cache: true,
            allow_unsafe_overwrite: Self::read_bool_env("S3_METADATA_ALLOW_UNSAFE_OVERWRITE"),
        };

        Ok(Arc::new(ObjectStoreMetadataClient::new(
            metadata_store,
            config,
        )))
    }

    fn resolve_cloud_provider(provider_override: Option<&str>) -> Result<CloudProvider> {
        if let Some(override_value) = provider_override.and_then(Self::normalize) {
            return Self::parse_provider("cloud-provider override", &override_value);
        }

        let cloud_provider = Self::read_env("CLOUD_PROVIDER");
        let storage_backend = Self::read_env("STORAGE_BACKEND");

        match (cloud_provider, storage_backend) {
            (Some(provider_raw), Some(backend_raw)) => {
                let provider = Self::parse_provider("CLOUD_PROVIDER", &provider_raw)?;
                let legacy = Self::parse_provider("STORAGE_BACKEND", &backend_raw)?;

                if provider != legacy {
                    warn!(
                        cloud_provider = %provider_raw,
                        storage_backend = %backend_raw,
                        "Ignoring STORAGE_BACKEND because CLOUD_PROVIDER is set"
                    );
                } else {
                    warn!("STORAGE_BACKEND is deprecated; remove it and keep CLOUD_PROVIDER only");
                }

                Ok(provider)
            }
            (Some(provider_raw), None) => Self::parse_provider("CLOUD_PROVIDER", &provider_raw),
            (None, Some(backend_raw)) => {
                warn!("STORAGE_BACKEND is deprecated; use CLOUD_PROVIDER");
                Self::parse_provider("STORAGE_BACKEND", &backend_raw)
            }
            (None, None) => Ok(CloudProvider::Memory),
        }
    }

    fn resolve_storage_container(
        provider: CloudProvider,
        container_override: Option<&str>,
    ) -> Result<String> {
        if let Some(container) = container_override.and_then(Self::normalize) {
            return Ok(container);
        }

        let common = Self::read_env("STORAGE_CONTAINER");

        match provider {
            CloudProvider::Memory => Ok(common.unwrap_or_else(|| "cardinalsin-data".to_string())),
            CloudProvider::Aws => {
                let legacy = Self::read_env("S3_BUCKET");

                match (common, legacy) {
                    (Some(container), Some(bucket)) => {
                        if container != bucket {
                            warn!(
                                storage_container = %container,
                                s3_bucket = %bucket,
                                "Ignoring S3 bucket because STORAGE_CONTAINER is set"
                            );
                        }
                        Ok(container)
                    }
                    (Some(container), None) => Ok(container),
                    (None, Some(bucket)) => {
                        warn!("S3_BUCKET is deprecated for selection; prefer STORAGE_CONTAINER");
                        Ok(bucket)
                    }
                    (None, None) => Err(crate::Error::Config(
                        "STORAGE_CONTAINER or S3_BUCKET required when CLOUD_PROVIDER=aws"
                            .to_string(),
                    )),
                }
            }
            CloudProvider::Gcp => {
                let google_bucket = Self::read_env("GOOGLE_BUCKET")
                    .or_else(|| Self::read_env("GOOGLE_BUCKET_NAME"));

                match (common, google_bucket) {
                    (Some(container), Some(bucket)) => {
                        if container != bucket {
                            warn!(
                                storage_container = %container,
                                google_bucket = %bucket,
                                "Ignoring GOOGLE_BUCKET because STORAGE_CONTAINER is set"
                            );
                        }
                        Ok(container)
                    }
                    (Some(container), None) => Ok(container),
                    (None, Some(bucket)) => Ok(bucket),
                    (None, None) => Err(crate::Error::Config(
                        "STORAGE_CONTAINER, GOOGLE_BUCKET, or GOOGLE_BUCKET_NAME required when CLOUD_PROVIDER=gcp".to_string(),
                    )),
                }
            }
            CloudProvider::Azure => {
                let azure_container = Self::read_env("AZURE_CONTAINER_NAME");

                match (common, azure_container) {
                    (Some(container), Some(azure_name)) => {
                        if container != azure_name {
                            warn!(
                                storage_container = %container,
                                azure_container_name = %azure_name,
                                "Ignoring AZURE_CONTAINER_NAME because STORAGE_CONTAINER is set"
                            );
                        }
                        Ok(container)
                    }
                    (Some(container), None) => Ok(container),
                    (None, Some(azure_name)) => Ok(azure_name),
                    (None, None) => Err(crate::Error::Config(
                        "STORAGE_CONTAINER or AZURE_CONTAINER_NAME required when CLOUD_PROVIDER=azure".to_string(),
                    )),
                }
            }
        }
    }

    fn parse_provider(source: &str, value: &str) -> Result<CloudProvider> {
        value
            .parse::<CloudProvider>()
            .map_err(|e| crate::Error::Config(format!("invalid {} '{}': {}", source, value, e)))
    }

    fn warn_ignored_provider_envs(selected_provider: CloudProvider) {
        const AWS_KEYS: &[&str] = &[
            "S3_BUCKET",
            "S3_REGION",
            "S3_ENDPOINT",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ];
        const GCP_KEYS: &[&str] = &[
            "GOOGLE_BUCKET",
            "GOOGLE_BUCKET_NAME",
            "GOOGLE_SERVICE_ACCOUNT",
            "GOOGLE_SERVICE_ACCOUNT_PATH",
            "GOOGLE_SERVICE_ACCOUNT_KEY",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "SERVICE_ACCOUNT",
            "SERVICE_ACCOUNT_PATH",
            "SERVICE_ACCOUNT_KEY",
        ];
        const AZURE_KEYS: &[&str] = &[
            "AZURE_CONTAINER_NAME",
            "AZURE_STORAGE_ACCOUNT_NAME",
            "AZURE_STORAGE_ACCOUNT_KEY",
            "AZURE_STORAGE_ACCESS_KEY",
            "AZURE_STORAGE_CLIENT_ID",
            "AZURE_STORAGE_CLIENT_SECRET",
            "AZURE_STORAGE_TENANT_ID",
            "AZURE_STORAGE_ENDPOINT",
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
            "AZURE_TENANT_ID",
        ];

        let mut ignored = Vec::new();

        match selected_provider {
            CloudProvider::Memory => {
                ignored.extend(Self::collect_set_env_vars(AWS_KEYS));
                ignored.extend(Self::collect_set_env_vars(GCP_KEYS));
                ignored.extend(Self::collect_set_env_vars(AZURE_KEYS));
            }
            CloudProvider::Aws => {
                ignored.extend(Self::collect_set_env_vars(GCP_KEYS));
                ignored.extend(Self::collect_set_env_vars(AZURE_KEYS));
            }
            CloudProvider::Gcp => {
                ignored.extend(Self::collect_set_env_vars(AWS_KEYS));
                ignored.extend(Self::collect_set_env_vars(AZURE_KEYS));
            }
            CloudProvider::Azure => {
                ignored.extend(Self::collect_set_env_vars(AWS_KEYS));
                ignored.extend(Self::collect_set_env_vars(GCP_KEYS));
            }
        }

        if ignored.is_empty() {
            return;
        }

        ignored.sort();
        ignored.dedup();

        warn!(
            provider = %selected_provider.as_str(),
            ignored_env_vars = %ignored.join(","),
            "Ignoring env vars for non-selected cloud providers"
        );
    }

    fn collect_set_env_vars(keys: &[&str]) -> Vec<String> {
        keys.iter()
            .copied()
            .filter_map(|key| Self::read_env(key).map(|_| key.to_string()))
            .collect()
    }

    fn normalize(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    fn read_env(key: &str) -> Option<String> {
        std::env::var(key).ok().and_then(|v| Self::normalize(&v))
    }

    fn read_bool_env(key: &str) -> bool {
        Self::read_env(key)
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};

    const TEST_ENV_KEYS: &[&str] = &[
        "CLOUD_PROVIDER",
        "STORAGE_BACKEND",
        "STORAGE_CONTAINER",
        "S3_BUCKET",
        "S3_REGION",
        "S3_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "GOOGLE_BUCKET",
        "GOOGLE_BUCKET_NAME",
        "AZURE_CONTAINER_NAME",
        "METADATA_BACKEND",
        "METADATA_CONTAINER",
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
            // SAFETY: Tests hold a global mutex to serialize environment mutation.
            unsafe { std::env::remove_var(key) };
        }

        for (key, value) in overrides {
            match value {
                Some(v) => {
                    // SAFETY: Tests hold a global mutex to serialize environment mutation.
                    unsafe { std::env::set_var(key, v) };
                }
                None => {
                    // SAFETY: Tests hold a global mutex to serialize environment mutation.
                    unsafe { std::env::remove_var(key) };
                }
            }
        }

        f();

        for (key, value) in saved {
            match value {
                Some(v) => {
                    // SAFETY: Tests hold a global mutex to serialize environment mutation.
                    unsafe { std::env::set_var(key, v) };
                }
                None => {
                    // SAFETY: Tests hold a global mutex to serialize environment mutation.
                    unsafe { std::env::remove_var(key) };
                }
            }
        }
    }

    #[test]
    fn resolve_storage_config_defaults_to_memory() {
        with_env(&[], || {
            let cfg = ComponentFactory::resolve_storage_config(None, None, "tenant-a").unwrap();
            assert_eq!(cfg.provider, CloudProvider::Memory);
            assert_eq!(cfg.container, "cardinalsin-data");
            assert_eq!(cfg.tenant_id, "tenant-a");
        });
    }

    #[test]
    fn resolve_storage_config_reads_cloud_provider() {
        with_env(
            &[
                ("CLOUD_PROVIDER", Some("gcp")),
                ("GOOGLE_BUCKET", Some("metrics-gcs")),
            ],
            || {
                let cfg = ComponentFactory::resolve_storage_config(None, None, "tenant-a").unwrap();
                assert_eq!(cfg.provider, CloudProvider::Gcp);
                assert_eq!(cfg.container, "metrics-gcs");
            },
        );
    }

    #[test]
    fn resolve_storage_config_uses_legacy_storage_backend_alias() {
        with_env(
            &[
                ("STORAGE_BACKEND", Some("s3")),
                ("S3_BUCKET", Some("legacy-bucket")),
            ],
            || {
                let cfg = ComponentFactory::resolve_storage_config(None, None, "tenant-a").unwrap();
                assert_eq!(cfg.provider, CloudProvider::Aws);
                assert_eq!(cfg.container, "legacy-bucket");
            },
        );
    }

    #[test]
    fn resolve_storage_config_prefers_cloud_provider_when_both_set() {
        with_env(
            &[
                ("CLOUD_PROVIDER", Some("gcp")),
                ("STORAGE_BACKEND", Some("s3")),
                ("GOOGLE_BUCKET", Some("gcp-bucket")),
                ("S3_BUCKET", Some("aws-bucket")),
            ],
            || {
                let cfg = ComponentFactory::resolve_storage_config(None, None, "tenant-a").unwrap();
                assert_eq!(cfg.provider, CloudProvider::Gcp);
                assert_eq!(cfg.container, "gcp-bucket");
            },
        );
    }

    #[test]
    fn resolve_storage_config_rejects_invalid_provider() {
        with_env(&[("CLOUD_PROVIDER", Some("digitalocean"))], || {
            let err = ComponentFactory::resolve_storage_config(None, None, "tenant-a")
                .expect_err("invalid provider should fail");
            assert!(
                err.to_string().contains("invalid CLOUD_PROVIDER"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn resolve_storage_config_override_wins() {
        with_env(
            &[
                ("CLOUD_PROVIDER", Some("aws")),
                ("STORAGE_CONTAINER", Some("env-bucket")),
            ],
            || {
                let cfg = ComponentFactory::resolve_storage_config(
                    Some("azure"),
                    Some("override-container"),
                    "tenant-a",
                )
                .unwrap();
                assert_eq!(cfg.provider, CloudProvider::Azure);
                assert_eq!(cfg.container, "override-container");
            },
        );
    }

    #[test]
    fn resolve_storage_container_aws_accepts_legacy_bucket() {
        with_env(
            &[
                ("CLOUD_PROVIDER", Some("aws")),
                ("S3_BUCKET", Some("legacy-aws-bucket")),
            ],
            || {
                let cfg = ComponentFactory::resolve_storage_config(None, None, "tenant-a").unwrap();
                assert_eq!(cfg.container, "legacy-aws-bucket");
            },
        );
    }

    #[test]
    fn resolve_storage_container_aws_requires_container() {
        with_env(&[("CLOUD_PROVIDER", Some("aws"))], || {
            let err = ComponentFactory::resolve_storage_config(None, None, "tenant-a")
                .expect_err("aws without container should fail");
            assert!(
                err.to_string()
                    .contains("STORAGE_CONTAINER or S3_BUCKET required"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn resolve_storage_container_gcp_requires_container_or_google_bucket() {
        with_env(&[("CLOUD_PROVIDER", Some("gcp"))], || {
            let err = ComponentFactory::resolve_storage_config(None, None, "tenant-a")
                .expect_err("gcp without bucket should fail");
            assert!(
                err.to_string().contains("GOOGLE_BUCKET"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn resolve_storage_container_azure_requires_container() {
        with_env(&[("CLOUD_PROVIDER", Some("azure"))], || {
            let err = ComponentFactory::resolve_storage_config(None, None, "tenant-a")
                .expect_err("azure without container should fail");
            assert!(
                err.to_string().contains("AZURE_CONTAINER_NAME"),
                "unexpected error: {err}"
            );
        });
    }

    #[test]
    fn create_object_store_memory_works() {
        with_env(
            &[
                ("CLOUD_PROVIDER", Some("memory")),
                ("STORAGE_CONTAINER", Some("ignored")),
            ],
            || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let cfg = ComponentFactory::resolve_storage_config(None, None, "tenant-a").unwrap();
                let store = rt.block_on(ComponentFactory::create_object_store_for(&cfg));
                assert!(store.is_ok(), "memory store should succeed: {store:?}");
            },
        );
    }

    #[test]
    fn create_metadata_client_local_backend() {
        with_env(&[("METADATA_BACKEND", Some("local"))], || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let cfg = StorageConfig {
                provider: CloudProvider::Memory,
                container: "data".to_string(),
                tenant_id: "tenant-a".to_string(),
            };
            let client = rt.block_on(ComponentFactory::create_metadata_client_for_storage(
                store, &cfg,
            ));
            assert!(client.is_ok(), "local metadata should succeed");
        });
    }

    #[test]
    fn create_metadata_client_object_store_backend() {
        with_env(
            &[
                ("METADATA_BACKEND", Some("object_store")),
                ("METADATA_CONTAINER", Some("meta-bucket")),
                ("METADATA_PREFIX", Some("metadata/")),
            ],
            || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let cfg = StorageConfig {
                    provider: CloudProvider::Memory,
                    container: "data".to_string(),
                    tenant_id: "tenant-a".to_string(),
                };
                let client = rt.block_on(ComponentFactory::create_metadata_client_for_storage(
                    store, &cfg,
                ));
                assert!(client.is_ok(), "object_store metadata should succeed");
            },
        );
    }

    #[test]
    fn create_metadata_client_accepts_s3_alias() {
        with_env(
            &[
                ("METADATA_BACKEND", Some("s3")),
                ("METADATA_BUCKET", Some("meta-bucket")),
            ],
            || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                let cfg = StorageConfig {
                    provider: CloudProvider::Memory,
                    container: "data".to_string(),
                    tenant_id: "tenant-a".to_string(),
                };
                let client = rt.block_on(ComponentFactory::create_metadata_client_for_storage(
                    store, &cfg,
                ));
                assert!(
                    client.is_ok(),
                    "s3 alias should map to object_store metadata"
                );
            },
        );
    }

    #[test]
    fn create_metadata_client_rejects_unknown_backend() {
        with_env(&[("METADATA_BACKEND", Some("sqlite"))], || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let cfg = StorageConfig {
                provider: CloudProvider::Memory,
                container: "data".to_string(),
                tenant_id: "tenant-a".to_string(),
            };
            match rt.block_on(ComponentFactory::create_metadata_client_for_storage(
                store, &cfg,
            )) {
                Ok(_) => panic!("unknown metadata backend should fail"),
                Err(err) => assert!(
                    err.to_string().contains("Unknown METADATA_BACKEND"),
                    "unexpected error: {err}"
                ),
            }
        });
    }
}
