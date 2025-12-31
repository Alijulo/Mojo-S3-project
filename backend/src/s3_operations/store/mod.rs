// store/mod.rs
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use std::pin::Pin;

#[derive(Clone, Debug)]
pub struct PutOptions {
    pub content_length: Option<u64>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub versioning: bool,
    pub extra: std::collections::HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct GetOptions {
    pub range: Option<(u64, u64)>,
    pub prefer_local: bool,
}

pub struct GetResult {
    pub content_length: u64,
    pub content_type: String,
    pub etag: String,
    pub version_id: Option<String>,
    pub body: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
}

// ──────────────────────────────────────────────────────
// ObjectStore trait
// ──────────────────────────────────────────────────────
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_stream(
        &self,
        bucket: &str,
        key: &str,
        opts: PutOptions,
        body: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
    ) -> Result<()>;

    async fn get_stream(
        &self,
        bucket: &str,
        key: &str,
        opts: GetOptions,
    ) -> Result<GetResult>;

    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, bucket: &str, key: &str) -> Result<()>;
    async fn exists(&self, bucket: &str, key: &str) -> Result<bool>;
}

// Re‑export implementations
pub use local::LocalStore;
pub use cluster::ClusterStore;

mod local;
mod cluster;
