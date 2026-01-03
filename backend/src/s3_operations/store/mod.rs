// store/mod.rs
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use std::pin::Pin;
use crate::s3_operations::handler_utils::AppError;
use crate::AppState;
use axum::{
    extract::State,
};
use std::sync::Arc;

// #[derive(Clone, Debug)]
// pub struct PutOptions {
//     pub content_length: Option<u64>,
//     pub content_type: Option<String>,
//     pub etag: Option<String>,
//     pub versioning: bool,
//     pub extra: std::collections::HashMap<String, String>,
// }
#[derive(Clone, Debug)]
pub struct PutOptions {
    /// Expected content length from Content-Length header
    pub content_length: Option<u64>,
    /// MIME type (e.g. application/json, image/png)
    pub content_type: Option<String>,
    /// Optional ETag provided by client (for integrity checks)
    pub etag: Option<String>,
    /// Whether bucket versioning is enabled
    pub versioning: bool,
    /// Conditional headers
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    /// User-defined metadata (x-amz-meta-*)
    pub user_meta: Option<std::collections::HashMap<String, String>>,
    /// Object tags (x-amz-tagging)
    pub tags: Option<std::collections::HashMap<String, String>>,
    /// Flexible catch-all for extra options
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
        state: Arc<AppState>,
    ) -> Result<(String, u64, Option<String>), AppError>;

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
