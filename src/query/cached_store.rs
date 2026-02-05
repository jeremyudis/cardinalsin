//! ObjectStore wrapper that integrates TieredCache
//!
//! This module provides a caching layer between DataFusion and the underlying
//! object storage (S3), using CardinalSin's tiered cache (RAM → NVMe).

use super::cache::TieredCache;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

/// ObjectStore wrapper that integrates TieredCache
///
/// This wrapper intercepts GET requests to use the tiered cache,
/// while delegating all other operations to the underlying store.
pub struct CachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<TieredCache>,
}

impl CachedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }
}

impl fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachedObjectStore({})", self.inner)
    }
}

impl fmt::Debug for CachedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedObjectStore")
            .field("inner", &self.inner.to_string())
            .finish()
    }
}

#[async_trait]
impl ObjectStore for CachedObjectStore {
    async fn put(&self, location: &Path, bytes: PutPayload) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let key = location.to_string();

        // Try cache first using get_or_fetch
        let cached_data = self
            .cache
            .get_or_fetch(&key, || async {
                // Cache miss - fetch from underlying store
                let result = self.inner.get(location).await?;
                let bytes = result.bytes().await?;
                Ok::<Bytes, crate::Error>(bytes)
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "CachedObjectStore",
                source: Box::new(e),
            })?;

        // Convert cached bytes to GetResult
        let bytes_clone = cached_data.bytes.clone();
        let size = cached_data.bytes.len();

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                async move { Ok(bytes_clone) },
            ))),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
            range: 0..size,
            attributes: Default::default(),
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        // For range requests or conditional gets, bypass cache
        if options.range.is_some() || options.if_match.is_some() || options.if_none_match.is_some()
        {
            return self.inner.get_opts(location, options).await;
        }

        // Otherwise use cached get
        self.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        // Range requests bypass cache for now
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        // Invalidate cache entry
        let key = location.to_string();
        self.cache.invalidate(&key).await;

        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        // Invalidate old cache entry
        let key = from.to_string();
        self.cache.invalidate(&key).await;

        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
