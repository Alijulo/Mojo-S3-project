use axum::{
    extract::{Path as AxumPath, State, Query,},
    response::{Response, IntoResponse},
    body::Body,
    http::{StatusCode, header, HeaderMap,HeaderValue},
    extract::Request,Extension
};
use std::{path::{Path, PathBuf},mem};
use tokio::{fs::{File, self}, sync::{Semaphore, Mutex}, io::{AsyncWriteExt,AsyncReadExt,AsyncWrite, AsyncSeekExt, AsyncRead,BufWriter}};
use tokio_util::io::ReaderStream;
use std::io;
use futures_util::StreamExt;
use std::collections::VecDeque;
use anyhow::{anyhow,Context};
use chrono::{DateTime, Utc};
use md5;
use hex;
use tracing::info;
use uuid::Uuid;
use crate::{
    s3_operations::{handler_utils::{AppError,ObjectInfo, CommonPrefix}, auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel,generate_presigned_url},metadata::{get, set},bucket_handlers::BucketMeta},
    AppState,S3_XMLNS, S3Headers,GLOBAL_IO_SEMAPHORE,DurabilityLevel, index::BucketIndex,
    index::types::{GetResult,IndexEntry},
};
use base64::Engine;
use serde::{Serialize, Deserialize,};
use http_range::HttpRange;
use httpdate::fmt_http_date;
use std::sync::atomic::{AtomicU64, Ordering,AtomicBool};
use md5::Context as Md5Context;
use http_body_util::BodyStream;


use once_cell::sync::Lazy;
use axum::body::Bytes; 
use std::{io::ErrorKind, time::{SystemTime,Instant}, collections::{HashSet,HashMap}, sync::Arc,convert::TryInto};
use sha2::{Digest, Sha256};
use base64::engine::general_purpose::STANDARD as BASE64;
use pin_project::pin_project;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

/// S3 List Objects Query Parameters
// ListObjectsV2 query
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ListObjectsQuery {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<u32>,                 // default 1000
    pub continuation_token: Option<String>,    // opaque
    pub start_after: Option<String>,           // exclusive start
    pub list_versions: Option<bool>,           // your flag (optional)
}

// Response structs (keep your existing ones; add continuation token fields for V2)
#[derive(Debug, serde::Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListBucketResult {
    #[serde(rename = "@xmlns")]
    pub xmlns: &'static str,
    #[serde(rename = "Name")]
    pub bucket_name: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Prefix")]
    pub prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Delimiter")]
    pub delimiter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "StartAfter")]
    pub start_after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "ContinuationToken")]
    pub continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "NextContinuationToken")]
    pub next_continuation_token: Option<String>,
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Contents")]
    pub objects: Vec<ObjectInfo>,
    #[serde(rename = "CommonPrefixes")]
    pub common_prefixes: Vec<CommonPrefix>,
}

#[derive(Serialize)]
struct ListVersionsResult {
    #[serde(rename = "Name")] name: String,
    #[serde(rename = "Prefix")] prefix: Option<String>,
    #[serde(rename = "KeyMarker")] key_marker: Option<String>,
    #[serde(rename = "MaxKeys")] max_keys: u32,
    #[serde(rename = "IsTruncated")] is_truncated: bool,
    #[serde(rename = "Version")] versions: Vec<VersionEntry>,
    #[serde(rename = "DeleteMarker")] delete_markers: Vec<DeleteMarkerEntry>,
    #[serde(rename = "CommonPrefixes")] common_prefixes: Vec<CommonPrefix>,
}

#[derive(Serialize)]
struct VersionEntry {
    #[serde(rename = "Key")] key: String,
    #[serde(rename = "VersionId")] version_id: String,
    #[serde(rename = "IsLatest")] is_latest: bool,
    #[serde(rename = "LastModified")] last_modified: String,
    #[serde(rename = "ETag")] etag: String,
    #[serde(rename = "Size")] size: u64,
}

#[derive(Serialize)]
struct DeleteMarkerEntry {
    #[serde(rename = "Key")] key: String,
    #[serde(rename = "VersionId")] version_id: String,
    #[serde(rename = "IsLatest")] is_latest: bool,
    #[serde(rename = "LastModified")] last_modified: String,
}



static PUT_BYTES: AtomicU64 = AtomicU64::new(0);
static PUT_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize,Clone, Default)]
pub struct ObjectMeta {
    #[serde(default)]
    etag: String,
    #[serde(default)]
    content_type: String,
    #[serde(default)]
    owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,   // opaque UUID
    #[serde(default)]
    is_delete_marker: bool,
    #[serde(default)]
    user_meta: std::collections::HashMap<String, String>,
    #[serde(default)]
    tags: std::collections::HashMap<String, String>,
}


impl ObjectMeta {
    pub fn new(owner: &str) -> Self {
        Self {
            etag: "".to_string(),
            content_type: "application/octet-stream".to_string(),
            owner: owner.to_string(),
            version_id: None,
            is_delete_marker: false,
            user_meta: Default::default(),
            tags: Default::default(),
        }
    }
}

/// Load object metadata from `.s3meta.<safe_key>` (xattr or .json)
pub async fn load_object_meta(parent: &Path, safe_key: &str) -> Result<ObjectMeta, AppError> {
    let marker = parent.join(format!(".s3meta.{}", safe_key));

    // Async get → .await
    let data = get(&marker, "user.s3.meta")
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("metadata read error: {e}")))?;

    // Your exact pattern
    data.and_then(|b| String::from_utf8(b).ok())
            .and_then(|s| serde_json::from_str(&s).ok())
            .ok_or(AppError::NoSuchKey)
}

/// Write object metadata sidecar via xattr, ensuring file exists and fsync on Unix
pub async fn save_object_meta(
    marker: &Path,
    obj_meta: &ObjectMeta,
    durability: bool,
) -> Result<(), AppError> {
    let obj_json = serde_json::to_string_pretty(obj_meta)
        .map_err(|e| AppError::Internal(e.into()))?;

    #[cfg(unix)]
    {
        if fs::metadata(marker).await.is_err() {
            let _ = fs::File::create(marker).await;
        }
    }

    set(marker, "user.s3.meta", obj_json.as_bytes())
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("set obj meta: {e}")))?;

    if durability {
        if let Some(parent) = marker.parent() {
            fsync_dir(parent).await;
        }
    }
    Ok(())
}

pub async fn load_bucket_meta(bucket_path: &Path) -> Result<BucketMeta, AppError> {
    let marker = bucket_path.join(".s3meta");
    let data = get(&marker, "user.s3.meta").await.ok().flatten();
    let json = data.and_then(|b| String::from_utf8(b).ok())
        .unwrap_or_else(|| r#"{"versioning":false,"object_count":0,"used_bytes":0}"#.to_string());
    serde_json::from_str(&json).map_err(|e| AppError::Internal(e.into()))
}

pub async fn save_bucket_meta(
    bucket_path: &Path,
    bucket_meta: &BucketMeta,
    durability: bool,
) -> Result<(), AppError> {
    let marker = bucket_path.join(".s3meta");
    let json = serde_json::to_string_pretty(bucket_meta)
        .map_err(|e| AppError::Internal(e.into()))?;

    #[cfg(unix)]
    {
        if fs::metadata(&marker).await.is_err() {
            let _ = fs::File::create(&marker).await;
        }
    }

    set(&marker, "user.s3.meta", json.as_bytes())
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("set bucket meta: {e}")))?;

    if durability {
        fsync_dir(bucket_path).await;
    }
    Ok(())
}



// URL-safe base64 token helpers (encode last returned key)
fn encode_token(s: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes())
}
fn decode_token(t: &str) -> Option<String> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(t.as_bytes())
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
}

// Small helper to parse x-amz-tagging: "k1=v1&k2=v2"
fn parse_tagging(s: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for pair in s.split('&') {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("").trim();
        let v = it.next().unwrap_or("").trim();
        if !k.is_empty() {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}




// Ensure helper exists in this module
#[cfg(unix)]
async fn ensure_file_exists(path: &Path) {
    if fs::metadata(path).await.is_err() {
        let _ = fs::File::create(path).await;
    }
}



// Validation helpers
fn validate_bucket(bucket: &str) -> Result<(), AppError> {
    let len = bucket.len();
    if len < 3 || len > 63 {
        return Err(AppError::InvalidBucketName("Invalid bucket length".into()));
    }
    if !bucket.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-') {
        return Err(AppError::InvalidBucketName("Invalid bucket characters".into()));
    }
    if bucket.starts_with('.') || bucket.ends_with('.') || bucket.contains("..") {
        return Err(AppError::InvalidBucketName("Invalid bucket dots".into()));
    }
    if bucket.split('.').all(|p| p.parse::<u8>().is_ok()) {
        return Err(AppError::InvalidBucketName("Bucket must not be formatted like an IP address".into()));
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<(), AppError> {
    if key.is_empty() {
        return Err(AppError::BadRequest("Empty object key".into()));
    }
    if key.contains("..") || key.starts_with('/') || key.contains('\\') {
        return Err(AppError::BadRequest("Invalid object key path".into()));
    }
    if key.chars().any(|c| c.is_control()) {
        return Err(AppError::BadRequest("Invalid object key control characters".into()));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadMarker {
    pub key: String,
    pub owner: String,
    pub upload_id: String,
    pub initiated: String,
    pub part_etags: HashMap<u32, String>,
    pub part_sizes: HashMap<u32, u64>,
    pub part_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPartQuery {
    pub partNumber: u32,
    pub uploadId: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteUploadQuery {
    pub uploadId: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbortUploadQuery {
    pub uploadId: String,
}

fn s3_now() -> String {
    DateTime::<Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
// Adapter that updates checksums while reading
#[pin_project]
pub struct ChecksumWriter<W> {
    #[pin]
    inner: W,
    md5: Md5Context,
    sha256: Sha256,
    total: u64,
}

impl<W: AsyncWrite + Unpin> ChecksumWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, md5: Md5Context::new(), sha256: Sha256::new(), total: 0 }
    }

    // Call after all writes; does NOT flush/sync
    pub fn finalize(self) -> (Vec<u8>, Vec<u8>, u64) {
        let md5_bytes = self.md5.finalize().0.to_vec();
        let sha256_bytes = self.sha256.finalize().to_vec();
        (md5_bytes, sha256_bytes, self.total)
    }

    // Strong finish for BufWriter<File> that flushes & syncs before returning checksums
    pub async fn finish(mut self) -> Result<(Vec<u8>, Vec<u8>, u64), AppError>
    where
        W: AsyncWrite + Unpin,
    {
        // Best-effort flush; sync handled by specialization below if BufWriter<File>
        self.inner
            .flush()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        Ok(self.finalize())
    }
}

// Specialization for BufWriter<File>: provide a strict finish that fsyncs the file.
impl ChecksumWriter<tokio::io::BufWriter<tokio::fs::File>> {
    pub async fn flush_and_sync(&mut self) -> Result<(), AppError> {
        self.inner.flush().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        let file_ref: &tokio::fs::File = self.inner.get_ref();
        file_ref.sync_all().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        Ok(())
    }

    pub async fn finish_and_sync(mut self) -> Result<(Vec<u8>, Vec<u8>, u64), AppError> {
        self.flush_and_sync().await?;
        Ok(self.finalize())
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for ChecksumWriter<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let poll = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(n)) = &poll {
            if *n > 0 {
                let written = &buf[..*n];
                self.md5.consume(written);
                self.sha256.update(written);
                self.total += *n as u64;
            }
        }
        poll
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let poll = std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let std::task::Poll::Ready(Ok(n)) = &poll {
            if *n > 0 {
                let mut remaining = *n;
                for s in bufs {
                    let take = remaining.min(s.len());
                    if take == 0 { break; }
                    let part = &s[..take];
                    self.md5.consume(part);
                    self.sha256.update(part);
                    remaining -= take;
                }
                self.total += *n as u64;
            }
        }
        poll
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        // Prefer flush before shutdown
        if let std::task::Poll::Ready(Ok(())) =
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        } else {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
}

// Important: Document usage
// This writer is safe with write_all; raw poll_write callers MUST handle partial writes.
// Vectored writes are supported and correctly hashed.

//TempGuard
struct TempGuard {
    path: PathBuf,
    committed: AtomicBool,
}

impl TempGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, committed: AtomicBool::new(false) }
    }

    fn mark_committed(&self) { self.committed.store(true, Ordering::Release); }

    async fn cleanup_async(&self) {
        if !self.committed.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.path).await;
        }
    }
}

impl Drop for TempGuard {
    fn drop(&mut self) {
        if !self.committed.load(Ordering::Acquire) {
            let path = self.path.clone();
            // Offload to an OS thread to avoid blocking the async reactor
            std::thread::spawn(move || {
                let _ = std::fs::remove_file(path);
            });
        }
    }
}



pub trait DurableOps {
    fn durability_enabled(&self) -> bool;
}

impl DurableOps for AppState {
    fn durability_enabled(&self) -> bool {
        self.durability == DurabilityLevel::Strong
    }
}

/// Best-effort fsync on a directory path.

/// Flush buffered writer and fsync its underlying file.
/// Best-effort fsync on a directory path (portable).
pub async fn fsync_dir(path: &Path) {
    #[cfg(unix)]
    {
        tokio::task::block_in_place(|| {
            let res = std::fs::File::open(path)
                .and_then(|f| {
                    // On Unix, opening a directory via std works; sync_all persists entry changes.
                    f.sync_all()
                });
            let _ = res;
        });
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        tokio::task::block_in_place(|| {
            let res = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(winapi::um::winbase::FILE_FLAG_BACKUP_SEMANTICS)
                .open(path)
                .and_then(|f| f.sync_all());
            let _ = res;
        });
    }
}

/// Flush buffered writer and fsync its underlying file.
pub async fn flush_and_sync_bufwriter(w: &mut tokio::io::BufWriter<tokio::fs::File>) -> Result<(), AppError> {
    w.flush().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    w.get_ref().sync_all().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    Ok(())
}

/// Atomic rename with optional fsync of both source and destination directories.
pub async fn atomic_rename(tmp: &Path, final_path: &Path, durability: bool) -> Result<(), AppError> {
    // Cross-device protection
    ensure_same_device(tmp, final_path).await?;

    fs::rename(tmp, final_path)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    if durability {
        if let Some(dst) = final_path.parent() { fsync_dir(dst).await; }
        if let Some(src) = tmp.parent() { fsync_dir(src).await; }
    }
    Ok(())
}

// Guard against cross-filesystem rename (atomicity breakage).
async fn ensure_same_device(a: &Path, b: &Path) -> Result<(), AppError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let (ma, mb) = tokio::task::block_in_place(|| {
            (std::fs::metadata(a), std::fs::metadata(b.parent().unwrap_or(Path::new("."))))
        });
        let dev_a = ma.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?.dev();
        let dev_b = mb.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?.dev();
        if dev_a != dev_b {
            return Err(AppError::Internal(anyhow::anyhow!("cross-device rename not allowed")));
        }
    }
    #[cfg(windows)]
    {
        // Minimal check: compare volume serial numbers via Win API if desired.
        // For now: assume objects must reside under the same bucket root; enforce via path prefix.
        let a_root = a.components().next();
        let b_root = b.components().next();
        if a_root != b_root {
            return Err(AppError::Internal(anyhow::anyhow!("cross-volume rename not allowed")));
        }
    }
    Ok(())
}

//Incomolete marker
fn spawn_incomplete_marker(parent: &Path, base_name: &std::ffi::OsStr) {
    let parent = parent.to_path_buf();
    let base_name = base_name.to_owned();
    let _ = tokio::spawn(async move {
        let marker = parent.join(format!(".incomplete.{}", base_name.to_string_lossy()));
        let _ = fs::write(&marker, b"post-rename failure").await;
        fsync_dir(&parent).await;
    });
}


/// A durable commit that sequences file flush+sync → rename → dir fsyncs → meta → bucket → index.
/// If any post-rename step fails, writes an "incomplete" marker and returns an error, allowing repair.
/// A durable commit that sequences file flush+sync → rename → dir fsyncs → meta → bucket → index.
/// If any post-rename step fails, writes an "incomplete" marker and returns an error, allowing repair.
pub async fn commit_object_transaction(
    state: &AppState,
    tmp_path: &Path,
    final_path: &Path,
    parent_dir: &Path,
    safe_key: &str,
    obj_meta: &ObjectMeta,
    bucket_path: &Path,
    bucket_meta: &BucketMeta,
    bucket_index: &Arc<BucketIndex>,
    object_size: u64,            // <-- explicit object size for index
) -> Result<(), AppError> {
    // Step 1: Atomic rename
    tracing::info!(tmp=?tmp_path, final=?final_path, "commit: starting atomic rename");
    atomic_rename(tmp_path, final_path, state.durability_enabled()).await?;

    // Step 2: Object metadata sidecar write
    let marker = bucket_path.join(format!(".s3meta.{}", safe_key));
    tracing::info!(marker=?final_path, "commit: writing object metadata sidecar");
    save_object_meta(&marker, obj_meta, state.durability_enabled())
        .await
        .map_err(|e| {
            tracing::error!(error=?e, marker=?final_path, "commit: failed to write object metadata");
            let _ = mark_incomplete(final_path, parent_dir);
            e
        })?;

    // Step 3: Bucket meta update (durable)
    tracing::info!(bucket=?bucket_path, "commit: updating bucket meta");
    save_bucket_meta(bucket_path, bucket_meta, state.durability_enabled())
        .await
        .map_err(|e| {
            tracing::info!(error=?e, bucket=?bucket_path, "commit: failed to update bucket meta");
            let _ = mark_incomplete(final_path, parent_dir);
            e
        })?;

    // Step 4: WAL index update
    use chrono::{DateTime, Utc};
    let last_modified = DateTime::<Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    let entry = IndexEntry {
        // Use logical S3 key, not file name
        key: final_path.file_name().unwrap().to_string_lossy().to_string(),
        size: object_size,
        etag: obj_meta.etag.clone(),
        last_modified,
        seq_no: 0, // assigned inside BucketIndex
        is_delete: obj_meta.is_delete_marker,
        version: obj_meta.version_id.clone(),
    };

    let key_for_log = entry.key.clone();

    bucket_index.put(entry).await
        .map_err(|e| {
            tracing::error!(error=?e, key=?key_for_log, "commit: failed to put index entry");
            let _ = mark_incomplete(final_path, parent_dir);
            AppError::Internal(anyhow::anyhow!(format!("index put: {e}")))
        })?;

    if state.durability_enabled() {
        if let Err(e) = bucket_index.compact().await {
            tracing::warn!("index compact failed after commit: {}", e);
        }
    }

    Ok(())
}



/// Write a small sidecar marker indicating incomplete state for repair tools.
fn mark_incomplete(final_path: &Path, parent_dir: &Path) -> Result<(), AppError> {
    let marker = parent_dir.join(format!(".incomplete.{}", final_path.file_name().unwrap().to_string_lossy()));
    tokio::task::block_in_place(|| {
        std::fs::write(&marker, b"post-rename failure").map_err(|e| AppError::Internal(anyhow::anyhow!(e)))
    })?;
    // Best-effort dir fsync
    futures::executor::block_on(fsync_dir(parent_dir));
    Ok(())
}

// Safe order: archive old BEFORE writing the new object, and fsync both file and dir.
async fn archive_previous_version(
    state: &AppState,
    base_path: &Path,
    parent: &Path,
    safe_key: &str,
    prev_version_id: &str,
) -> Result<(), AppError> {
    let existed = fs::metadata(base_path).await.is_ok();
    if !existed { return Ok(()); }

    let archive_dir = parent.join(".s3versions").join(safe_key);
    fs::create_dir_all(&archive_dir).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    let archive_path = archive_dir.join(prev_version_id);
    // Prefer hard link; fallback to copy.
    if fs::hard_link(base_path, &archive_path).await.is_err() {
        fs::copy(base_path, &archive_path).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    }
    // Fsync the archived file and the versions directory if strong durability.
    if state.durability_enabled() {
        let f = fs::File::open(&archive_path).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        f.sync_all().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        fsync_dir(&archive_dir).await;
    }
    Ok(())
}


pub async fn put_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: Extension<AuthenticatedUser>,
    req: axum::extract::Request,
) -> Result<(StatusCode, HeaderMap), AppError> {
    // ────────────────────────────────
    // SECTION 0: Metrics + Validation
    // ────────────────────────────────
    PUT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let start = std::time::Instant::now();

    validate_bucket(&bucket)?;
    validate_key(&key)?;

    let req_id = format!("put-{}-{}", bucket, key);
    tracing::info!(req_id, bucket=%bucket, key=%key, "PUT begin");

    // ────────────────────────────────
    // SECTION 1: Authorization
    // ────────────────────────────────
    let allowed = check_bucket_permission(
        &state.pool,
        &user.0,
        &bucket,
        PermissionLevel::ReadWrite.as_str(),
    )
    .await
    .map_err(AppError::Internal)?;
    if !allowed {
        return Err(AppError::AccessDenied);
    }

    // ────────────────────────────────
    // SECTION 2: Concurrency Controls
    // ────────────────────────────────
    let _permit = state.io_budget.acquire().await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let _obj_guard = state.io_locks.lock(format!("{bucket}/{key}")).await;

    // ────────────────────────────────
    // SECTION 3: Paths + Bucket Meta
    // ────────────────────────────────
    let base_path = state.object_path(&bucket, &key);
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }
    let parent = base_path.parent()
        .ok_or_else(|| AppError::BadRequest("Invalid object path".into()))?
        .to_path_buf();
    fs::create_dir_all(&parent).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    let mut bucket_meta = load_bucket_meta(&bucket_path).await?;
    let versioning = bucket_meta.versioning;

    // ────────────────────────────────
    // SECTION 4: Safe Key + Object Meta
    // ────────────────────────────────
    let safe_key_base = encode_token(&key);
    let safe_key = if safe_key_base.len() <= 200 {
        safe_key_base
    } else {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        format!("sha256-{}", hex::encode(hasher.finalize()))
    };
    //let meta_marker = parent.join(format!(".s3meta.{safe_key}"));
    let meta_marker = key.replace('/', "_");
    //let safe_key = key.replace('/', "_");

    let mut obj_meta = load_object_meta(&parent, &safe_key)
        .await
        .unwrap_or_else(|_| ObjectMeta::new(&user.0.username));

    // ────────────────────────────────
    // SECTION 5: Request Headers
    // ────────────────────────────────
    let (parts, body) = req.into_parts();
    let headers = parts.headers;

    let content_type = headers.get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let content_len = headers.get(header::CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let max_bytes = std::env::var("MAX_OBJECT_BYTES")
        .ok().and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5 * 1024 * 1024 * 1024); // default 5 GiB

    if let Some(len) = content_len {
        if len > max_bytes {
            return Err(AppError::EntityTooLarge);
        }
    }

    let content_md5_b64 = headers.get("Content-MD5").and_then(|h| h.to_str().ok()).map(str::to_string);
    let x_amz_sha256_hex = headers.get("x-amz-content-sha256").and_then(|h| h.to_str().ok()).map(str::to_string);

    // ────────────────────────────────
    // SECTION 6: User Metadata + Tags
    // ────────────────────────────────
    let mut user_meta = std::collections::HashMap::new();
    let mut user_meta_wire_bytes: usize = 0;
    for (name, value) in headers.iter() {
        if let Some(rest) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(vs) = value.to_str() {
                user_meta_wire_bytes += "x-amz-meta-".len() + rest.len() + vs.len();
                user_meta.insert(rest.to_string(), vs.to_string());
            }
        }
    }
    // AWS S3 hard limit: total metadata size <= 2 KB
    if user_meta_wire_bytes > 2 * 1024 {
        return Err(AppError::BadRequest("metadata too large".into()));
    }

    let tags = headers.get("x-amz-tagging")
        .and_then(|h| h.to_str().ok())
        .map(parse_tagging)
        .unwrap_or_default();

    // ────────────────────────────────
    // SECTION 7: Existence + Conditional Headers
    // ────────────────────────────────
    let meta_opt = fs::metadata(&base_path).await.ok();
    let existed = meta_opt.is_some();
    let old_size = meta_opt.map(|m| m.len()).unwrap_or(0);
    let is_new_object = !existed;

    let object_exists = existed;
    let current_etag_unquoted = obj_meta.etag.trim().trim_matches('"');

    if let Some(if_match) = headers.get(header::IF_MATCH).and_then(|h| h.to_str().ok()) {
        let expected = if_match.trim();
        if expected == "*" {
            if !object_exists {
                return Err(AppError::PreconditionFailed);
            }
        } else {
            let expected_unquoted = expected.trim_matches('"');
            if current_etag_unquoted.is_empty() || current_etag_unquoted != expected_unquoted {
                return Err(AppError::PreconditionFailed);
            }
        }
    }
    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH).and_then(|h| h.to_str().ok()) {
        let forbidden = if_none_match.trim();
        if forbidden == "*" {
            if object_exists {
                return Err(AppError::PreconditionFailed);
            }
        } else {
            let forbidden_unquoted = forbidden.trim_matches('"');
            if object_exists && current_etag_unquoted == forbidden_unquoted {
                return Err(AppError::PreconditionFailed);
            }
        }
    }

    // ────────────────────────────────
    // SECTION 8: Versioning + Archive
    // ────────────────────────────────
    let new_version_id = if versioning { Some(Uuid::new_v4().to_string()) } else { None };

    if versioning && existed {
        let prev_version_id = obj_meta.version_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        let archive_dir = parent.join(".s3versions").join(&safe_key);
        fs::create_dir_all(&archive_dir).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        let archive_path = archive_dir.join(prev_version_id);
        if fs::hard_link(&base_path, &archive_path).await.is_err() {
            fs::copy(&base_path, &archive_path).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        }
        if state.durability_enabled() {
            let f = fs::File::open(&archive_path).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
            f.sync_all().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
            fsync_dir(&archive_dir).await;
        }
    }

    // ────────────────────────────────
    // SECTION 9: Temp File + Streaming Write
    // ────────────────────────────────
    let tmp_path = parent.join(format!("{}.{}.tmp", safe_key, Uuid::new_v4()));
    let file = fs::File::create(&tmp_path).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let guard = TempGuard::new(tmp_path.clone());
    let inner_writer = tokio::io::BufWriter::with_capacity(8 * 1024 * 1024, file);
    let mut writer = ChecksumWriter::new(inner_writer);
    let mut total_size = 0u64;
    let mut body_stream = body.into_data_stream();
    while let Some(chunk_res) = body_stream.next().await {
        let chunk = match chunk_res {
            Ok(c) => c,
            Err(e) => {
                guard.cleanup_async().await;
                return Err(AppError::Internal(anyhow::anyhow!(format!("read chunk: {e}"))));
            }
        };
        let bytes = chunk.as_ref();
        if !bytes.is_empty() {
            if total_size.saturating_add(bytes.len() as u64) > max_bytes {
                guard.cleanup_async().await;
                return Err(AppError::EntityTooLarge);
            }
            if let Err(e) = writer.write_all(bytes).await {
                guard.cleanup_async().await;
                return Err(AppError::Internal(anyhow::anyhow!(format!("write chunk: {e}"))));
            }
            total_size += bytes.len() as u64;
        }
    }
    if let Some(expected_len) = content_len {
        if total_size != expected_len {
            guard.cleanup_async().await;
            return Err(AppError::BadRequest("Content-Length mismatch".into()));
        }
    }

    // ────────────────────────────────
    // SECTION 10: Flush + Checksums
    // ────────────────────────────────
    if state.durability_enabled() {
        writer.flush_and_sync().await?;
    } else {
        writer.flush().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    }
    let (md5_bytes, sha256_bytes, _) = writer.finalize();
    let etag = format!("\"{}\"", hex::encode(&md5_bytes));
    if let Some(md5_b64) = content_md5_b64.as_ref() {
        let computed_b64 = base64::engine::general_purpose::STANDARD.encode(&md5_bytes);
        if &computed_b64 != md5_b64 {
            guard.cleanup_async().await;
            return Err(AppError::BadDigest("Content-MD5 mismatch".into()));
        }
    }
    if let Some(sha256_hex) = x_amz_sha256_hex.as_ref() {
        if sha256_hex != "UNSIGNED-PAYLOAD" {
            let computed_hex = hex::encode(&sha256_bytes);
            if &computed_hex != sha256_hex {
                guard.cleanup_async().await;
                return Err(AppError::Sha256Mismatch);
            }
        }
    }

    // ────────────────────────────────
    // SECTION 11: Prepare metadata and bucket meta deltas
    // ────────────────────────────────
    obj_meta.etag = etag.clone();
    obj_meta.content_type = content_type;
    obj_meta.owner = user.0.username.clone();
    obj_meta.is_delete_marker = false;
    obj_meta.user_meta = user_meta;
    obj_meta.tags = tags;
    obj_meta.version_id = new_version_id.clone();

    if is_new_object {
        bucket_meta.object_count = bucket_meta.object_count.saturating_add(1);
    }
    if versioning {
        bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(total_size);
    } else {
        if total_size >= old_size {
            bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(total_size - old_size);
        } else {
            bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_sub(old_size - total_size);
        }
    }

    // ────────────────────────────────
    // SECTION 12: Obtain WAL index and run atomic commit
    // ────────────────────────────────
    let idx = state.get_bucket_index(&bucket)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open index: {e}"))))?;

    // Atomic commit: flush+sync already done; now rename → meta sidecar → bucket meta → WAL index
    commit_object_transaction(
        &state,
        &tmp_path,
        &base_path,
        &parent,
        &meta_marker,      // sidecar metadata path (.s3meta.<safe_key>)
        &obj_meta,
        &bucket_path,
        &bucket_meta,
        &idx,
        total_size,        // explicit object size for index entry
    ).await?;

    // ────────────────────────────────
    // SECTION 13: Metrics
    // ────────────────────────────────
    PUT_BYTES.fetch_add(total_size, std::sync::atomic::Ordering::Relaxed);

    // ────────────────────────────────
    // SECTION 14: Finalize + Response
    // ────────────────────────────────
    guard.mark_committed();

    let mut resp_headers = S3Headers::common_headers();
    resp_headers.insert(
        header::ETAG,
        HeaderValue::from_str(&etag)
            .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("invalid ETAG header: {e}"))))?,
    );
    resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    if let Some(v) = new_version_id.as_ref() {
        resp_headers.insert(
            "x-amz-version-id",
            HeaderValue::from_str(v)
                .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("invalid version-id header: {e}"))))?,
        );
    }
    resp_headers.insert(
        "x-amz-request-id",
        HeaderValue::from_str(&req_id)
            .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("invalid request-id header: {e}"))))?,
    );

    tracing::info!(
        req_id,
        bucket=%bucket,
        key=%key,
        bytes=%total_size,
        ms=%start.elapsed().as_millis(),
        etag=%etag,
        version_id=?new_version_id,
        "PUT commit success"
    );

    Ok((StatusCode::OK, resp_headers))
}





/// Copy an object within or across buckets with durability, versioning, metadata, and index updates.
/// Reads the source file, streams into a temp file while computing MD5/SHA256, then commits atomically.
/// Honors conditional copy headers: x-amz-copy-source, x-amz-copy-source-if-match, x-amz-copy-source-if-none-match.
/// Expects helpers: ChecksumWriter, TempGuard, atomic_rename (fsync both dirs), fsync_dir, 
/// load/save_object_meta, load/save_bucket_meta, load/save_bucket_index, archive_previous_version,
/// commit_object_transaction, encode_token, validate_bucket, validate_key, DurableOps.

pub async fn copy_object(
    State(state): State<Arc<AppState>>,
    AxumPath((dst_bucket, dst_key)): AxumPath<(String, String)>,
    user: Extension<AuthenticatedUser>,
    req: axum::extract::Request,
) -> Result<(StatusCode, HeaderMap), AppError> {
    // ────────────────────────────────
    // SECTION 0: Metrics + Validation
    // ────────────────────────────────
    PUT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let start = std::time::Instant::now();
    validate_bucket(&dst_bucket)?;
    validate_key(&dst_key)?;

    // ────────────────────────────────
    // SECTION 1: Parse Headers
    // ────────────────────────────────
    let (parts, _body) = req.into_parts();
    let headers = parts.headers;
    let copy_src = headers
        .get("x-amz-copy-source")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| AppError::BadRequest("Missing x-amz-copy-source".into()))?
        .trim()
        .trim_start_matches('/');

    let mut src_parts = copy_src.splitn(2, '/');
    let src_bucket = src_parts.next().ok_or_else(|| AppError::BadRequest("Invalid source bucket".into()))?.to_string();
    let src_key = src_parts.next().ok_or_else(|| AppError::BadRequest("Invalid source key".into()))?.to_string();
    validate_bucket(&src_bucket)?;
    validate_key(&src_key)?;

    let req_id = format!("copy-{}:{} -> {}:{}", src_bucket, src_key, dst_bucket, dst_key);
    tracing::info!(req_id, src_bucket=%src_bucket, src_key=%src_key, dst_bucket=%dst_bucket, dst_key=%dst_key, "COPY begin");

    // ────────────────────────────────
    // SECTION 2: Authorization
    // ────────────────────────────────
    let allowed_src = check_bucket_permission(&state.pool, &user.0, &src_bucket, PermissionLevel::ReadOnly.as_str())
        .await.map_err(AppError::Internal)?;
    let allowed_dst = check_bucket_permission(&state.pool, &user.0, &dst_bucket, PermissionLevel::ReadWrite.as_str())
        .await.map_err(AppError::Internal)?;
    if !allowed_src || !allowed_dst {
        return Err(AppError::AccessDenied);
    }

    // ────────────────────────────────
    // SECTION 3: Concurrency Controls
    // ────────────────────────────────
    let _permit = state.io_budget.acquire().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let _src_guard = state.io_locks.lock(format!("{}/{}", src_bucket, src_key)).await;
    let _dst_guard = state.io_locks.lock(format!("{}/{}", dst_bucket, dst_key)).await;

    // ────────────────────────────────
    // SECTION 4: Paths + Bucket Meta
    // ────────────────────────────────
    let src_base = state.object_path(&src_bucket, &src_key);
    let dst_base = state.object_path(&dst_bucket, &dst_key);
    let src_bucket_path = state.bucket_path(&src_bucket);
    let dst_bucket_path = state.bucket_path(&dst_bucket);
    if !src_bucket_path.exists() || !dst_bucket_path.exists() {
        return Err(AppError::NotFound("Bucket not found".into()));
    }
    if fs::metadata(&src_base).await.is_err() {
        return Err(AppError::NoSuchKey);
    }
    let dst_parent = dst_base.parent().ok_or_else(|| AppError::BadRequest("Invalid destination path".into()))?.to_path_buf();
    fs::create_dir_all(&dst_parent).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    let mut dst_bucket_meta = load_bucket_meta(&dst_bucket_path).await?;
    let versioning = dst_bucket_meta.versioning;
    let dst_safe_key = encode_token(&dst_key);
    let src_safe_key = encode_token(&src_key);

    //let dst_meta_marker = dst_parent.join(format!(".s3meta.{dst_safe_key}"));
    let dst_meta_marker = dst_key.replace('/', "_");
    let src_meta = load_object_meta(&src_base.parent().unwrap_or(&src_bucket_path), &src_safe_key)
        .await.unwrap_or_else(|_| ObjectMeta::new(&user.0.username));

    // ────────────────────────────────
    // SECTION 5: Conditional Copy Checks
    // ────────────────────────────────
    let current_etag_unquoted = src_meta.etag.trim().trim_matches('"');
    if let Some(if_match) = headers.get("x-amz-copy-source-if-match").and_then(|h| h.to_str().ok()) {
        let expected = if_match.trim().trim_matches('"');
        if !expected.is_empty() && current_etag_unquoted != expected {
            return Err(AppError::PreconditionFailed);
        }
    }
    if let Some(if_none_match) = headers.get("x-amz-copy-source-if-none-match").and_then(|h| h.to_str().ok()) {
        let forbidden = if_none_match.trim().trim_matches('"');
        if !forbidden.is_empty() && current_etag_unquoted == forbidden {
            return Err(AppError::PreconditionFailed);
        }
    }

    // ────────────────────────────────
    // SECTION 6: Versioning + Archive
    // ────────────────────────────────
    let new_version_id = if versioning { Some(Uuid::new_v4().to_string()) } else { None };
    if versioning {
        let prev_version_id = load_object_meta(&dst_parent, &dst_safe_key)
            .await.ok().and_then(|m| m.version_id)
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        archive_previous_version(&state, &dst_base, &dst_parent, &dst_safe_key, &prev_version_id).await?;
    }

    // ────────────────────────────────
    // SECTION 7: Copy Stream
    // ────────────────────────────────
    let tmp_path = dst_parent.join(format!("{}.{}.tmp", dst_safe_key, Uuid::new_v4()));
    let dst_file = fs::File::create(&tmp_path).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let guard = TempGuard::new(tmp_path.clone());
    let dst_buf = tokio::io::BufWriter::with_capacity(8 * 1024 * 1024, dst_file);
    let mut writer = ChecksumWriter::new(dst_buf);
    let mut src_file = fs::File::open(&src_base).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let mut total_size = 0u64;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = src_file.read(&mut buf).await.map_err(|e| {
            futures::executor::block_on(guard.cleanup_async());
            AppError::Internal(anyhow::anyhow!(format!("read source: {e}")))
        })?;
        if n == 0 { break; }
        writer.write_all(&buf[..n]).await.map_err(|e| {
            futures::executor::block_on(guard.cleanup_async());
            AppError::Internal(anyhow::anyhow!(format!("write dst: {e}")))
        })?;
        total_size += n as u64;
    }

    // ────────────────────────────────
    // SECTION 8: Flush + Checksums
    // ────────────────────────────────
    if state.durability_enabled() {
        writer.flush_and_sync().await?;
    } else {
        writer.flush().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    }
    let (md5_bytes, _sha256_bytes, _) = writer.finalize();
    let etag = format!("\"{}\"", hex::encode(&md5_bytes));


    // ────────────────────────────────
    // SECTION 9: Destination Meta + Bucket Stats
    // ────────────────────────────────
    let mut dst_obj_meta = src_meta.clone();
    dst_obj_meta.etag = etag.clone();
    dst_obj_meta.owner = user.0.username.clone();
    dst_obj_meta.version_id = new_version_id.clone();
    dst_obj_meta.is_delete_marker = false;

    let dst_existed = fs::metadata(&dst_base).await.is_ok();
    let old_size = if dst_existed {
        fs::metadata(&dst_base).await.map(|m| m.len()).unwrap_or(0)
    } else { 0 };

    if !dst_existed {
        dst_bucket_meta.object_count = dst_bucket_meta.object_count.saturating_add(1);
    }
    if versioning {
        dst_bucket_meta.used_bytes = dst_bucket_meta.used_bytes.saturating_add(total_size);
    } else {
        if total_size >= old_size {
            dst_bucket_meta.used_bytes = dst_bucket_meta.used_bytes.saturating_add(total_size - old_size);
        } else {
            dst_bucket_meta.used_bytes = dst_bucket_meta.used_bytes.saturating_sub(old_size - total_size);
        }
    }

    // ────────────────────────────────
    // SECTION 10: Transactional Commit (rename → meta → bucket → WAL index)
    // ────────────────────────────────
    let idx = state.get_bucket_index(&dst_bucket)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open index: {e}"))))?;

    commit_object_transaction(
        &state,
        &tmp_path,          // temp file path
        &dst_base,          // final destination object path
        &dst_parent,        // parent dir for incomplete marker/fsync
        &dst_meta_marker,   // destination sidecar .s3meta.<safe_key>
        &dst_obj_meta,      // prepared destination metadata
        &dst_bucket_path,
        &dst_bucket_meta,
        &idx,               // WAL-backed index
        total_size,         // explicit object size
    ).await?;

    // Mark temp guard committed only after successful transaction
    guard.mark_committed();

    PUT_BYTES.fetch_add(total_size, std::sync::atomic::Ordering::Relaxed);

    // ────────────────────────────────
    // SECTION 11: Response
    // ────────────────────────────────
    let mut resp_headers = S3Headers::common_headers();
    resp_headers.insert(
        header::ETAG,
        HeaderValue::from_str(&etag)
            .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("invalid ETAG: {e}"))))?,
    );
    resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    if let Some(v) = new_version_id.as_ref() {
        resp_headers.insert(
            "x-amz-version-id",
            HeaderValue::from_str(v)
                .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("invalid version-id: {e}"))))?,
        );
    }
    resp_headers.insert(
        "x-amz-request-id",
        HeaderValue::from_str(&req_id)
            .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("invalid request-id: {e}"))))?,
    );

    tracing::info!(
        req_id,
        src_bucket=%src_bucket,
        src_key=%src_key,
        dst_bucket=%dst_bucket,
        dst_key=%dst_key,
        bytes=%total_size,
        ms=%start.elapsed().as_millis(),
        etag=%etag,
        version_id=?new_version_id,
        "COPY commit success"
    );

    Ok((StatusCode::OK, resp_headers))
}






//GET OBJECT OPERATION
pub async fn get_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    user: Extension<AuthenticatedUser>,
    _req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    // 0. Global IO permit
    let _io_permit = GLOBAL_IO_SEMAPHORE.acquire().await.unwrap();
    info!("GET {}/{}?{:?}", bucket, key, params);

    // 1. Permission
    let can_read = check_bucket_permission(
        &state.pool,
        &user.0,
        &bucket,
        PermissionLevel::ReadOnly.as_str(),
    ).await?;
    let can_rw = check_bucket_permission(
        &state.pool,
        &user.0,
        &bucket,
        PermissionLevel::ReadWrite.as_str(),
    ).await?;
    if !can_read && !can_rw {
        return Err(AppError::AccessDenied);
    }

    // 2. Paths
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }
    let full_key_path = state.object_path(&bucket, &key);
    let parent = full_key_path.parent().unwrap().to_path_buf();
    let safe_key = key.replace('/', "_");

    // 3. Common prefix → empty 200
    if full_key_path.is_dir() && params.get("prefix").map_or(true, |p| key.starts_with(p)) {
        let mut hdrs = S3Headers::common_headers();
        hdrs.insert(axum::http::header::CONTENT_LENGTH, "0".parse().unwrap());
        return Ok((StatusCode::OK, hdrs, Body::empty()).into_response());
    }

    // 4. Bucket versioning flag
    let versioning = get(&bucket_path.join(".s3meta"), "user.s3.meta")
        .await
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v.get("versioning")?.as_bool())
        .unwrap_or(false);

    // 5. Consult WAL-backed index first
    // let idx = state.get_bucket_index(&bucket).await
    //     .map_err(|e| AppError::Internal(anyhow!(format!("open index: {e}"))))?;
    // match idx.get(&key).await {
    //     GetResult::Deleted(_) => return Err(AppError::NoSuchKey),
    //     GetResult::NotFound => return Err(AppError::NoSuchKey),
    //     GetResult::Found(_entry) => {
    //         // continue; we still load object_meta for content_type, tags, etc.
    //     }
    // }
    // 5. Consult WAL-backed index first
    let idx = state.get_bucket_index(&bucket).await
        .map_err(|e| AppError::Internal(anyhow!(format!("open index: {e}"))))?;

    match idx.get(&key).await {
        GetResult::Deleted(seq) => {
            tracing::warn!(
                bucket=%bucket,
                key=%key,
                seq_no=?seq,
                "GET: object marked deleted in WAL index"
            );
            return Err(AppError::NoSuchKey);
        }
        GetResult::NotFound => {
            tracing::warn!(
                bucket=%bucket,
                key=%key,
                "GET: object not found in WAL index"
            );
            return Err(AppError::NoSuchKey);
        }
        GetResult::Found(entry) => {
            tracing::info!(
                bucket=%bucket,
                key=%key,
                etag=?entry.etag,
                size=?entry.size,
                version=?entry.version,
                "GET: object found in WAL index"
            );
            // continue; we still load object_meta for content_type, tags, etc.
        }
    }


    // 6. Load object metadata
    let obj_meta = load_object_meta(&parent, &safe_key).await?;
    let req_version = params.get("versionId").cloned();

    // // 7. Version resolution
    // let object_path = if versioning {
    //     match (&req_version, obj_meta.version_id.clone(), obj_meta.is_delete_marker) {
    //         (None, _, true) => return Err(AppError::NoSuchKey),
    //         (Some(vid), Some(latest_vid), true) if &latest_vid == vid => return Err(AppError::NoSuchKey),
    //         (Some(vid), Some(latest_vid), false) if &latest_vid == vid => full_key_path.clone(),
    //         (Some(vid), _, _) => {
    //             let archive_path = parent.join(".s3versions").join(&safe_key).join(vid);
    //             if !archive_path.exists() {
    //                 return Err(AppError::NotFound(format!("version {}", vid)));
    //             }
    //             archive_path
    //         }
    //         (None, _, false) => full_key_path.clone(),
    //     }
    // } else {
    //     full_key_path.clone()
    // };

    // 7. Version resolution
    tracing::info!(
        bucket=%bucket,
        key=%key,
        versioning=%versioning,
        req_version=?req_version,
        obj_version=?obj_meta.version_id,
        delete_marker=?obj_meta.is_delete_marker,
        "GET: starting version resolution"
    );

    let object_path = if versioning {
        match (&req_version, obj_meta.version_id.clone(), obj_meta.is_delete_marker) {
            (None, _, true) => {
                tracing::warn!(bucket=%bucket, key=%key, "GET: object has delete marker, returning NoSuchKey");
                return Err(AppError::NoSuchKey);
            }
            (Some(vid), Some(latest_vid), true) if &latest_vid == vid => {
                tracing::warn!(bucket=%bucket, key=%key, version=?vid, "GET: requested version is a delete marker, returning NoSuchKey");
                return Err(AppError::NoSuchKey);
            }
            (Some(vid), Some(latest_vid), false) if &latest_vid == vid => {
                tracing::info!(bucket=%bucket, key=%key, version=?vid, "GET: serving latest version");
                full_key_path.clone()
            }
            (Some(vid), _, _) => {
                let archive_path = parent.join(".s3versions").join(&safe_key).join(vid);
                if !archive_path.exists() {
                    tracing::error!(bucket=%bucket, key=%key, version=?vid, path=?archive_path, "GET: requested version not found");
                    return Err(AppError::NotFound(format!("version {}", vid)));
                }
                tracing::info!(bucket=%bucket, key=%key, version=?vid, path=?archive_path, "GET: serving archived version");
                archive_path
            }
            (None, _, false) => {
                tracing::info!(bucket=%bucket, key=%key, "GET: serving current version");
                full_key_path.clone()
            }
        }
    } else {
        tracing::info!(bucket=%bucket, key=%key, "GET: versioning disabled, serving current object");
        full_key_path.clone()
    };


    // 8. Open file
    // let mut file = File::open(&object_path).await.map_err(|e| {
    //     if e.kind() == std::io::ErrorKind::NotFound {
    //         AppError::NoSuchKey
    //     } else {
    //         AppError::Io(e)
    //     }
    // }).context("open file")?;
// 8. Open file
    tracing::info!(
        bucket=%bucket,
        key=%key,
        path=?object_path,
        "GET: attempting to open object file"
    );

    let mut file = File::open(&object_path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            tracing::error!(
                bucket=%bucket,
                key=%key,
                path=?object_path,
                "GET: object file not found"
            );
            AppError::NoSuchKey
        } else {
            tracing::error!(
                bucket=%bucket,
                key=%key,
                path=?object_path,
                error=?e,
                "GET: failed to open object file"
            );
            AppError::Io(e)
        }
    }).context("open file")?;

    let file_meta = file.metadata().await.context("file metadata")?;
    let total_size = file_meta.len();

    tracing::info!(
        bucket=%bucket,
        key=%key,
        path=?object_path,
        size=total_size,
        modified=?file_meta.modified().ok(),
        "GET: successfully opened object file"
    );
    // 9. Conditional headers
    if let Some(if_none) = headers.get(axum::http::header::IF_NONE_MATCH).and_then(|h| h.to_str().ok()) {
        if if_none.split(',').any(|tag| tag.trim() == obj_meta.etag) {
            let mut hdrs = S3Headers::common_headers();
            hdrs.insert(axum::http::header::ETAG, obj_meta.etag.parse().unwrap());
            if versioning {
                if let Some(v) = obj_meta.version_id.clone() {
                    hdrs.insert("x-amz-version-id", v.parse().unwrap());
                }
            }
            return Ok((StatusCode::NOT_MODIFIED, hdrs, Body::empty()).into_response());
        }
    }
    if let Some(if_match) = headers.get(axum::http::header::IF_MATCH).and_then(|h| h.to_str().ok()) {
        let any_match = if_match.split(',').any(|tag| tag.trim() == obj_meta.etag);
        if !any_match {
            return Ok(StatusCode::PRECONDITION_FAILED.into_response());
        }
    }

    // 10. Range parsing
    let range_header = headers.get(axum::http::header::RANGE).and_then(|v| v.to_str().ok());
    let (start, end, is_partial) = if let Some(range) = range_header {
        let ranges = HttpRange::parse(range, total_size)
            .map_err(|_| AppError::InvalidArgument("Invalid Range".into()))?;
        if ranges.len() != 1 {
            return Err(AppError::InvalidArgument("Multiple ranges not supported".into()));
        }
        let r = &ranges[0];
        (r.start, r.start + r.length - 1, true)
    } else {
        (0u64, total_size.saturating_sub(1), false)
    };
    let content_length = end - start + 1;

    // 11. Platform safety
    if total_size > usize::MAX as u64 {
        return Err(AppError::Internal(anyhow!(
            "File too large for platform (max {} bytes)",
            usize::MAX
        )));
    }

    // 12. sendfile fast path
    let use_sendfile = cfg!(unix) && !is_partial && content_length == total_size;
    if use_sendfile {
        let mut resp = axum::response::Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, &obj_meta.content_type)
            .header(axum::http::header::ACCEPT_RANGES, "bytes")
            .header(axum::http::header::ETAG, &obj_meta.etag)
            .header(
                axum::http::header::LAST_MODIFIED,
                HeaderValue::from_str(&httpdate::fmt_http_date(file_meta.modified()?)).unwrap(),
            )
            .header(axum::http::header::CONTENT_LENGTH, total_size.to_string());
        if versioning {
            if let Some(v) = obj_meta.version_id.clone() {
                resp = resp.header("x-amz-version-id", v);
            }
        }
        let mut response = resp.body(Body::empty()).map_err(|e| AppError::Internal(anyhow!(e)))?;
        if try_sendfile(&mut file, 0, total_size, &mut response).await? {
            info!("sendfile served {object_path:?} | {total_size} bytes | ETag: {}", obj_meta.etag);
            return Ok(response);
        }
    }

    // 13. Linux readahead hint
    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::{posix_fadvise, PosixFadviseAdvice};
        let fd = file.as_raw_fd();
        let _ = posix_fadvise(fd, 0, 0, PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL);
    }
    // 14. Seek for range
    if is_partial {
        file.seek(std::io::SeekFrom::Start(start)).await.context("seek")?;
    }

    // 15. Stream response
    const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
    let stream = ReaderStream::with_capacity(file, CHUNK_SIZE);
    let body = Body::from_stream(stream);
    let response_content_length = content_length;

    // 16. Headers
    let mut resp_headers = S3Headers::common_headers();
    resp_headers.insert(
        axum::http::header::CONTENT_TYPE,
        obj_meta.content_type.parse().unwrap(),
    );
    resp_headers.insert(
        axum::http::header::ACCEPT_RANGES,
        "bytes".parse().unwrap(),
    );
    resp_headers.insert(
        axum::http::header::ETAG,
        obj_meta.etag.parse().unwrap(),
    );
    resp_headers.insert(
        axum::http::header::LAST_MODIFIED,
        HeaderValue::from_str(&httpdate::fmt_http_date(file_meta.modified()?)).unwrap(),
    );
    if versioning {
        if let Some(v) = obj_meta.version_id.clone() {
            resp_headers.insert("x-amz-version-id", v.parse().unwrap());
        }
    }
    if is_partial {
        resp_headers.insert(
            axum::http::header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
        );
    }
    resp_headers.insert(
        axum::http::header::CONTENT_LENGTH,
        response_content_length.to_string().parse().unwrap(),
    );

    // 17. Status
    let status = if is_partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    info!(
        "Serving {object_path:?} | {response_content_length} bytes | Range: {is_partial} | ETag: {}",
        obj_meta.etag
    );

    Ok((status, resp_headers, body).into_response())
}



// -------- sendfile helper (unchanged behavior) --------
#[cfg(unix)]
async fn try_sendfile(
    file: &mut File,
    offset: u64,
    len: u64,
    response: &mut axum::response::Response,
) -> Result<bool, AppError> {
    use libc::{c_int, off_t, sendfile, ssize_t};

    unsafe extern "C" fn raw_sendfile(out_fd: c_int, in_fd: c_int, offset: *mut off_t, count: usize) -> ssize_t {
        sendfile(out_fd, in_fd, offset, count)
    }

    let socket_fd = response
        .extensions()
        .get::<hyper::server::conn::Http>()
        .and_then(|conn| conn.io().as_raw_fd())
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("No socket for sendfile")))?;

    let file_fd = file.as_raw_fd();
    let mut cur_off = offset as off_t;
    let mut remaining = len as usize;

    while remaining > 0 {
        let sent = unsafe { raw_sendfile(socket_fd, file_fd, &mut cur_off, remaining) };
        if sent <= 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Ok(false); // fallback to ReaderStream
        }
        remaining -= sent as usize;
    }
    Ok(true)
}

#[cfg(not(unix))]
async fn try_sendfile(
    _file: &mut File,
    _offset: u64,
    _len: u64,
    _response: &mut axum::response::Response,
) -> Result<bool, AppError> {
    Ok(false)
}


//Presign_object  Generate presigned URL
pub async fn presign_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: Extension<AuthenticatedUser>,
) -> Result<impl IntoResponse, AppError> {
    // Permission check
    let allowed = check_bucket_permission(
        &state.pool,
        &user.0,
        &bucket,
        PermissionLevel::ReadOnly.as_str()
    ).await?;
    if !allowed {
        return Err(AppError::AccessDenied);
    }

    // Generate presigned URL
    let secret = std::env::var("PRESIGN_SECRET")
        .unwrap_or_else(|_| "default-secret".into());
    let url = generate_presigned_url(&bucket, &key, &secret, 300); // 5 min expiry

    Ok((StatusCode::OK, url))
}




// -----------------------------------------------------------------------------
//  S3 LIST Objects Operation
// -----------------------------------------------------------------------------
pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    Query(query): Query<ListObjectsQuery>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    tracing::info!("LIST-V2 {bucket} {query:?} user={}", user.0.username);

    // Permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    let root = state.bucket_path(&bucket);
    if !root.exists() {
        return Err(AppError::NotFound(bucket));
    }

    // Query normalization
    let prefix = query.prefix.clone().unwrap_or_default();
    let delimiter = query.delimiter.clone().unwrap_or_default();
    let max_keys_s3: u32 = query.max_keys.unwrap_or(1000).min(1000);
    let max_keys: usize = max_keys_s3 as usize;

    // Resolve start position
    let mut start_after_key = query.start_after.clone().unwrap_or_default();
    if let Some(tok) = &query.continuation_token {
        if let Some(decoded) = decode_token(tok) {
            start_after_key = decoded;
        }
    }

    // --- Load entries from WAL-backed index ---
    let idx = state.get_bucket_index(&bucket).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open index: {e}"))))?;
    let results = idx.list_prefix(&prefix).await;

    // Convert GetResult → IndexEntry-like
    let mut entries: Vec<(String, String, u64, String)> = Vec::new();
    for res in results {
        match res {
            GetResult::Found(entry) => {
                entries.push((
                    entry.key.clone(),
                    entry.last_modified.clone(),
                    entry.size,
                    entry.etag.clone(),
                ));
            }
            GetResult::Deleted(_) => {
                // skip tombstoned keys
            }
            GetResult::NotFound => {}
        }
    }

    // --- Filter + paginate ---
    let mut objects = Vec::new();
    let mut prefixes = std::collections::HashSet::new();
    let mut next_token: Option<String> = None;
    let mut emitted = 0usize;
    let mut last_key_emitted: Option<String> = None;

    for (key, last_modified, size, etag) in entries.into_iter().filter(|(k, _, _, _)| k > &start_after_key) {
        if !prefix.is_empty() && !key.starts_with(&prefix) {
            continue;
        }

        // delimiter handling
        if !delimiter.is_empty() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                if let Some(pos) = rest.find(&delimiter) {
                    prefixes.insert(format!("{}{}", prefix, &rest[..pos + delimiter.len()]));
                    continue;
                }
            }
        }

        objects.push(ObjectInfo {
            key: key.clone(),
            last_modified,
            size_bytes: size,
            etag,
        });

        emitted += 1;
        last_key_emitted = Some(key.clone());

        if emitted >= max_keys {
            break;
        }
    }

    // Determine truncation and nextContinuationToken
    let is_truncated = emitted >= max_keys;
    if is_truncated {
        if let Some(last) = &last_key_emitted {
            next_token = Some(encode_token(last));
        }
    }

    let result = ListBucketResult {
        xmlns: S3_XMLNS,
        bucket_name: bucket,
        prefix: (!prefix.is_empty()).then_some(prefix),
        delimiter: (!delimiter.is_empty()).then_some(delimiter),
        start_after: query.start_after.clone().filter(|s| !s.is_empty()),
        continuation_token: query.continuation_token.clone(),
        next_continuation_token: next_token.clone(),
        max_keys: max_keys_s3,
        is_truncated,
        objects,
        common_prefixes: prefixes.into_iter().map(|p| CommonPrefix { prefix: p }).collect(),
    };

    let xml = quick_xml::se::to_string(&result)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("XML: {e}")))?;

    let mut headers = S3Headers::xml_headers();
    headers.insert(header::CONTENT_LENGTH, xml.len().to_string().parse().unwrap());
    if let Some(tok) = next_token {
        headers.insert("x-amz-next-continuation-token", tok.parse().unwrap());
    }

    Ok((headers, xml))
}








//DELETE OPERATION
pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    tracing::info!("DELETE {bucket}/{key} user={}", user.0.username);

    // 1) Permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    // 2) Paths
    let final_path = state.object_path(&bucket, &key);
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }
    let parent = final_path.parent().unwrap().to_path_buf();
    let safe_key = key.replace('/', "_");
    let meta_marker = parent.join(format!(".s3meta.{safe_key}"));

    // 3) Load bucket meta
    let bucket_meta_path = bucket_path.join(".s3meta");
    let bucket_meta_json = get(&bucket_meta_path, "user.s3.meta")
        .await
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .unwrap_or_else(|| r#"{"versioning":false,"object_count":0,"used_bytes":0}"#.to_string());
    let mut bucket_meta: BucketMeta = serde_json::from_str(&bucket_meta_json).unwrap_or_default();
    let versioning = bucket_meta.versioning;

    // 4) Load object meta
    let mut obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
        .await
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

    let old_size = match fs::metadata(&final_path).await {
        Ok(m) => m.len(),
        Err(_) => 0,
    };

    // 5) Versioning behavior
    let mut version_id = None;
    if versioning {
        // Mark delete marker in object meta
        version_id = Some(Uuid::new_v4().to_string());
        obj_meta.is_delete_marker = true;
        obj_meta.version_id = version_id.clone();

        let obj_json = serde_json::to_string_pretty(&obj_meta)
            .map_err(|e| AppError::Internal(anyhow::Error::new(e)))?;
        set(&meta_marker, "user.s3.meta", obj_json.as_bytes()).await?;
    } else {
        // Physically delete object + metadata
        let _ = fs::remove_file(&final_path).await;
        let _ = fs::remove_file(&meta_marker).await;
    }

    // 6) Update bucket stats
    if old_size > 0 {
        bucket_meta.object_count = bucket_meta.object_count.saturating_sub(1);
        bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_sub(old_size);
    }
    let bucket_json = serde_json::to_string_pretty(&bucket_meta)
        .map_err(|e| AppError::Internal(anyhow::Error::new(e)))?;
    set(&bucket_meta_path, "user.s3.meta", bucket_json.as_bytes()).await?;

    // 7) WAL-backed index update (append tombstone)
    use chrono::Utc;
    let last_modified = DateTime::<Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    let idx = state.get_bucket_index(&bucket).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open index: {e}"))))?;

    let tombstone = IndexEntry {
        key: key.clone(),
        size: 0,
        etag: "".to_string(),
        last_modified,
        seq_no: 0, // assigned inside BucketIndex
        is_delete: true,
        version: version_id.clone(),
    };

    idx.put(tombstone).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("index tombstone: {e}"))))?;

    if state.durability_enabled() {
        if let Err(e) = idx.compact().await {
            tracing::warn!("index compact failed after delete: {}", e);
        }
    }

    // 8) Response
    let mut headers = S3Headers::common_headers();
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    if let Some(v) = version_id {
        headers.insert("x-amz-version-id", HeaderValue::from_str(&v).unwrap());
    }
    headers.insert(
        "x-amz-request-id",
        HeaderValue::from_str(&format!("del-{}-{}", bucket, key)).unwrap(),
    );

    Ok((StatusCode::NO_CONTENT, headers))
}



//metapath TODO to be implemented
fn object_meta_path(parent: &Path, key: &str) -> PathBuf {
    let safe_key = key.replace('/', "_");
    #[cfg(windows)]
    {
        parent.join(format!(".s3meta.{safe_key}.json"))
    }
    #[cfg(unix)]
    {
        parent.join(format!(".s3meta.{safe_key}"))
    }
}


//HEAD OBJECT
pub async fn head_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<Response, AppError> {
    tracing::info!("HEAD {bucket}/{key} user={}", user.0.username);

    // 1) Permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    // 2) Paths
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }
    let full_key_path = state.object_path(&bucket, &key);
    let parent = full_key_path.parent().unwrap().to_path_buf();
    let safe_key = key.replace('/', "_");
    let meta_marker = parent.join(format!(".s3meta.{safe_key}"));

    // 3) Bucket versioning flag
    let versioning = get(&bucket_path.join(".s3meta"), "user.s3.meta")
        .await
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v.get("versioning")?.as_bool())
        .unwrap_or(false);

    // 4) Load object metadata
    let obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
        .await
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

    // 5) Delete marker handling
    if obj_meta.is_delete_marker {
        return Err(AppError::NoSuchKey);
    }

    // 6) Filesystem metadata
    let metadata = fs::metadata(&full_key_path).await.map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            AppError::NoSuchKey
        } else {
            AppError::Io(e)
        }
    })?;
    let content_length = metadata.len();
    let modified_time: SystemTime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let datetime: DateTime<Utc> = modified_time.into();
    let last_modified_str = datetime.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    // 7) Build response headers
    let mut resp_headers = S3Headers::common_headers();
    resp_headers.insert(header::ETAG, obj_meta.etag.parse().unwrap());
    resp_headers.insert(header::CONTENT_TYPE, obj_meta.content_type.parse().unwrap());
    resp_headers.insert(header::CONTENT_LENGTH, content_length.to_string().parse().unwrap());
    resp_headers.insert(header::LAST_MODIFIED, last_modified_str.parse().unwrap());
    if versioning {
        if let Some(v) = obj_meta.version_id.clone() {
            resp_headers.insert("x-amz-version-id", v.parse().unwrap());
        }
    }

    // 8) Response (attach headers via builder.header(...) in a loop)
    let mut builder = Response::builder().status(StatusCode::OK);
    for (name, value) in resp_headers.iter() {
        builder = builder.header(name, value.clone());
    }
    let response = builder
        .body(Body::empty())
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to build HEAD response: {e}")))?
        .into_response();

    tracing::info!(
        "HEAD success: {} ({} bytes), ETag: {}, User={}",
        full_key_path.display(),
        content_length,
        obj_meta.etag,
        user.0.username
    );

    Ok(response)
}


//---------------------------------
//MULT-PART UPLOAD
//---------------------------------
pub async fn initiate_multipart_upload(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap, String), AppError> {
    // ────────────────────────────────
    // SECTION 0: Validation + AuthZ
    // ────────────────────────────────
    validate_bucket(&bucket)?;
    validate_key(&key)?;

    let allowed = check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await.map_err(AppError::Internal)?;
    if !allowed {
        return Err(AppError::AccessDenied);
    }

    // ────────────────────────────────
    // SECTION 1: Paths + marker
    // ────────────────────────────────
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket.clone()));
    }
    let uploads_dir = bucket_path.join(".s3uploads");
    fs::create_dir_all(&uploads_dir).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    let upload_id = Uuid::new_v4().to_string();
    let marker_path = uploads_dir.join(format!("{upload_id}.json"));

    let marker = MultipartUploadMarker {
        key: key.clone(),
        owner: user.0.username.clone(),
        upload_id: upload_id.clone(),
        initiated: s3_now(),
        part_etags: HashMap::new(),
        part_sizes: HashMap::new(),
        part_count: 0,
    };
    let data = serde_json::to_vec(&marker).map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    set(&marker_path, "s3.upload", &data).await?;

    // WAL entry for initiation (optional, useful for replay/cleanup)
    let idx = state.get_bucket_index(&bucket).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open index: {e}"))))?;
    idx.put(IndexEntry {
        key: format!("{key}"),
        size: 0,
        etag: "".into(),
        last_modified: marker.initiated.clone(),
        seq_no: 0,
        is_delete: false,
        version: Some(upload_id.clone()),
    }).await.map_err(|e| AppError::Internal(anyhow::anyhow!(format!("index put: {e}"))))?;

    // ────────────────────────────────
    // SECTION 2: Response
    // ────────────────────────────────
    let xml = format!(
        "<InitiateMultipartUploadResult>\
            <Bucket>{}</Bucket>\
            <Key>{}</Key>\
            <UploadId>{}</UploadId>\
        </InitiateMultipartUploadResult>",
        bucket, key, upload_id
    );
    let mut headers = S3Headers::xml_headers();
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_str(&xml.len().to_string()).unwrap());
    Ok((StatusCode::OK, headers, xml))
}



pub async fn upload_part(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(params): Query<UploadPartQuery>,
    user: Extension<AuthenticatedUser>,
    req: Request<Body>,   // full Request
) -> Result<(StatusCode, HeaderMap), AppError> {
    // ────────────────────────────────
    // SECTION 0: Validation + AuthZ
    // ────────────────────────────────
    validate_bucket(&bucket)?;
    validate_key(&key)?;
    if params.partNumber == 0 {
        return Err(AppError::BadRequest("partNumber must be >= 1".into()));
    }

    let allowed = check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await.map_err(AppError::Internal)?;
    if !allowed {
        return Err(AppError::AccessDenied);
    }

    // Concurrency budget + lock per (bucket/key/uploadId)
    let _permit = state.io_budget.acquire().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let _guard = state.io_locks.lock(format!("{}/{}/{}", bucket, key, params.uploadId)).await;

    // ────────────────────────────────
    // SECTION 1: Split request into headers + body
    // ────────────────────────────────
    let (parts, body) = req.into_parts();
    let headers = parts.headers;
    let mut stream = BodyStream::new(body);

    // Resolve marker and part path
    let bucket_path = state.bucket_path(&bucket);
    let uploads_dir = bucket_path.join(".s3uploads");
    let marker_path = uploads_dir.join(format!("{}.json", params.uploadId));
    let marker_json = get(&marker_path, "s3.upload").await?.ok_or(AppError::NoSuchUpload)?;
    let mut marker: MultipartUploadMarker = serde_json::from_slice(&marker_json)
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("bad marker: {e}"))))?;
    if marker.key != key {
        return Err(AppError::BadRequest("uploadId does not match key".into()));
    }

    let parts_dir = uploads_dir.join(&params.uploadId);
    fs::create_dir_all(&parts_dir).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let part_path = parts_dir.join(format!("part-{:010}.tmp", params.partNumber));

    // ────────────────────────────────
    // SECTION 2: Stream write + checksum
    // ────────────────────────────────
    let dst_file = fs::File::create(&part_path).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let mut writer = ChecksumWriter::new(tokio::io::BufWriter::with_capacity(8 * 1024 * 1024, dst_file));
    let mut total_size = 0u64;

    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(|e| AppError::Internal(anyhow::anyhow!(format!("read body: {e}"))))?;
        if let Some(data) = frame.data_ref() {
            writer.write_all(data).await
                .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("write part: {e}"))))?;
            total_size += data.len() as u64;
        }
    }

    if state.durability_enabled() {
        writer.flush_and_sync().await?;
    } else {
        writer.flush().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    }
    let (md5_bytes, _, _) = writer.finalize();
    let etag = format!("\"{}\"", hex::encode(&md5_bytes));

    // ────────────────────────────────
    // SECTION 2b: Validate Content-MD5 header (if present)
    // ────────────────────────────────
    if let Some(content_md5) = headers.get("Content-MD5") {
        let provided = content_md5.to_str().map_err(|_| AppError::BadRequest("invalid Content-MD5 header".into()))?;
        let expected = BASE64.encode(&md5_bytes);
        if provided != expected {
            return Err(AppError::BadDigest(format!(
                "Content-MD5 mismatch: expected {}, got {}",
                expected, provided
            )));
        }
    }

    // ────────────────────────────────
    // SECTION 3: Update marker + WAL
    // ────────────────────────────────
    marker.part_etags.insert(params.partNumber, etag.clone());
    marker.part_sizes.insert(params.partNumber, total_size);
    marker.part_count = marker.part_etags.len() as u32;

    let updated = serde_json::to_vec(&marker).map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    set(&marker_path, "s3.upload", &updated).await?;

    let idx = state.get_bucket_index(&bucket).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open index: {e}"))))?;
    idx.put(IndexEntry {
        key: format!("{}:part-{}", key, params.partNumber),
        size: total_size,
        etag: etag.clone(),
        last_modified: s3_now(),
        seq_no: 0,
        is_delete: false,
        version: Some(params.uploadId.clone()),
    }).await.map_err(|e| AppError::Internal(anyhow::anyhow!(format!("index put: {e}"))))?;

    // ────────────────────────────────
    // SECTION 4: Response
    // ────────────────────────────────
    let mut headers = S3Headers::common_headers();
    headers.insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    Ok((StatusCode::OK, headers))
}

pub async fn complete_multipart_upload(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(params): Query<CompleteUploadQuery>, // { uploadId }
    user: Extension<AuthenticatedUser>,
    xml_body: Bytes,            // XML with <Part><PartNumber>..</PartNumber><ETag>..</ETag></Part>*
) -> Result<(StatusCode, HeaderMap, String), AppError> {
    // ────────────────────────────────
    // SECTION 0: Validation + AuthZ + Locks
    // ────────────────────────────────
    validate_bucket(&bucket)?;
    validate_key(&key)?;
    let allowed = check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await.map_err(AppError::Internal)?;
    if !allowed {
        return Err(AppError::AccessDenied);
    }
    let _permit = state.io_budget.acquire().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let _guard = state.io_locks.lock(format!("{}/{}/{}", bucket, key, params.uploadId)).await;

    // ────────────────────────────────
    // SECTION 1: Load marker + client XML parts
    // ────────────────────────────────
    #[derive(Debug, Clone, Deserialize)]
    struct XmlPart {
        #[serde(rename = "PartNumber")]
        part_number: u32,
        #[serde(rename = "ETag")]
        etag: String,
    }
    #[derive(Debug, Clone, Deserialize)]
    struct CompleteMultipartUpload {
        #[serde(rename = "Part")]
        parts: Vec<XmlPart>,
    }

    let bucket_path = state.bucket_path(&bucket);
    let uploads_dir = bucket_path.join(".s3uploads");
    let marker_path = uploads_dir.join(format!("{}.json", params.uploadId));
    let marker_json = get(&marker_path, "s3.upload").await?.ok_or(AppError::NoSuchUpload)?;
    let marker: MultipartUploadMarker = serde_json::from_slice(&marker_json)
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("bad marker: {e}"))))?;
    if marker.key != key {
        return Err(AppError::BadRequest("uploadId does not match key".into()));
    }

    // Parse client XML body
    let client: CompleteMultipartUpload = quick_xml::de::from_reader(xml_body.as_ref())
        .map_err(|e| AppError::BadRequest(format!("invalid CompleteMultipartUpload XML: {e}")))?;

    if client.parts.is_empty() {
        return Err(AppError::BadRequest("no parts in CompleteMultipartUpload".into()));
    }

    // Validate ascending order and presence
    let mut part_numbers: Vec<u32> = client.parts.iter().map(|p| p.part_number).collect();
    let mut sorted = part_numbers.clone();
    sorted.sort_unstable();
    if part_numbers != sorted {
        return Err(AppError::BadRequest("parts must be in ascending PartNumber order".into()));
    }

    // Validate each part exists in marker and ETag matches
    for p in &client.parts {
        let expected = marker.part_etags.get(&p.part_number)
            .ok_or_else(|| AppError::BadRequest(format!("missing uploaded part {}", p.part_number)))?;
        let expected_clean = expected.trim_matches('"');
        let provided_clean = p.etag.trim().trim_matches('"');
        if expected_clean != provided_clean {
            return Err(AppError::BadRequest(format!(
                "etag mismatch for part {}: expected {}, got {}",
                p.part_number, expected_clean, provided_clean
            )));
        }
    }

    // ────────────────────────────────
    // SECTION 2: Concatenate parts into final object
    // ────────────────────────────────
    let final_path = state.object_path(&bucket, &key);
    let parent = final_path.parent().ok_or_else(|| AppError::BadRequest("invalid dst path".into()))?.to_path_buf();
    fs::create_dir_all(&parent).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    let safe_key = encode_token(&key);
    let tmp_path = parent.join(format!("{}.{}.tmp", safe_key, Uuid::new_v4()));
    let dst_file = fs::File::create(&tmp_path).await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let mut writer = ChecksumWriter::new(tokio::io::BufWriter::with_capacity(8 * 1024 * 1024, dst_file));

    let parts_dir = uploads_dir.join(&params.uploadId);
    let mut total_size = 0u64;
    let mut buf = vec![0u8; 1024 * 1024];

    for pn in &sorted {
        let p = parts_dir.join(format!("part-{:010}.tmp", pn));
        let mut f = fs::File::open(&p).await
            .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open part {}: {e}", pn))))?;
        loop {
            let n = f.read(&mut buf).await
                .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("read part {}: {e}", pn))))?;
            if n == 0 { break; }
            writer.write_all(&buf[..n]).await
                .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("write final: {e}"))))?;
            total_size += n as u64;
        }
    }

    if state.durability_enabled() {
        writer.flush_and_sync().await?;
    } else {
        writer.flush().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    }
    // We compute and keep the file checksums internally but S3-style ETag is not plain MD5 for multipart
    let (_md5_bytes_unused, _, _) = writer.finalize();

    // ────────────────────────────────
    // SECTION 2b: S3-style multipart ETag calculation ("<md5sum>-N")
    // ────────────────────────────────
    let mut md5_concat = Vec::new();
    for pn in &sorted {
        let et = marker.part_etags.get(pn).unwrap();
        let raw = et.trim_matches('"');
        let bytes = hex::decode(raw)
            .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("bad etag hex for part {}: {e}", pn))))?;
        md5_concat.extend_from_slice(&bytes);
    }
    let multipart_md5 = md5::compute(&md5_concat);
    let etag = format!("\"{}-{}\"", hex::encode(multipart_md5.0), sorted.len());

    // ────────────────────────────────
    // SECTION 3: Metadata + bucket stats + atomic rename
    // ────────────────────────────────
    let mut obj_meta = ObjectMeta::new(&user.0.username);
    obj_meta.etag = etag.clone();
    obj_meta.is_delete_marker = false;
    obj_meta.version_id = None; // optionally assign if bucket versioning is enabled

    // Bucket meta
    let mut bucket_meta = load_bucket_meta(&bucket_path).await?;
    let existed = fs::metadata(&final_path).await.is_ok();
    if !existed {
        bucket_meta.object_count = bucket_meta.object_count.saturating_add(1);
        bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(total_size);
    } else {
        let old_size = fs::metadata(&final_path).await.map(|m| m.len()).unwrap_or(0);
        if total_size >= old_size {
            bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(total_size - old_size);
        } else {
            bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_sub(old_size - total_size);
        }
    }

    atomic_rename(&tmp_path, &final_path, state.durability_enabled()).await?;

    let meta_marker = parent.join(format!(".s3meta.{safe_key}"));
    save_object_meta(&meta_marker, &obj_meta, state.durability_enabled()).await?;
    save_bucket_meta(&bucket_path, &bucket_meta, state.durability_enabled()).await?;

    // ────────────────────────────────
    // SECTION 4: WAL index update
    // ────────────────────────────────
    let idx = state.get_bucket_index(&bucket).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open index: {e}"))))?;
    idx.put(IndexEntry {
        key: key.clone(),
        size: total_size,
        etag: etag.clone(),
        last_modified: s3_now(),
        seq_no: 0,            // assigned internally
        is_delete: false,
        version: Some(params.uploadId.clone()),
    }).await.map_err(|e| AppError::Internal(anyhow::anyhow!(format!("index put: {e}"))))?;
    if state.durability_enabled() {
        if let Err(e) = idx.compact().await {
            tracing::warn!("index compact after complete failed: {}", e);
        }
    }

    // ────────────────────────────────
    // SECTION 5: Cleanup temp parts + marker
    // ────────────────────────────────
    let _ = fs::remove_dir_all(&parts_dir).await;
    let _ = fs::remove_file(&marker_path).await;

    // ────────────────────────────────
    // SECTION 6: Response
    // ────────────────────────────────
    let xml = format!(
        "<CompleteMultipartUploadResult>\
            <Bucket>{}</Bucket>\
            <Key>{}</Key>\
            <ETag>{}</ETag>\
        </CompleteMultipartUploadResult>",
        bucket, key, etag
    );
    let mut headers = S3Headers::xml_headers();
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_str(&xml.len().to_string()).unwrap());
    headers.insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
    Ok((StatusCode::OK, headers, xml))
}




pub async fn abort_multipart_upload(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(params): Query<AbortUploadQuery>,
    user: Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    // ────────────────────────────────
    // SECTION 0: Validation + AuthZ + Lock
    // ────────────────────────────────
    validate_bucket(&bucket)?;
    validate_key(&key)?;
    let allowed = check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await.map_err(AppError::Internal)?;
    if !allowed {
        return Err(AppError::AccessDenied);
    }
    let _permit = state.io_budget.acquire().await.map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    let _guard = state.io_locks.lock(format!("{}/{}/{}", bucket, key, params.uploadId)).await;

    // ────────────────────────────────
    // SECTION 1: Paths + marker
    // ────────────────────────────────
    let bucket_path = state.bucket_path(&bucket);
    let uploads_dir = bucket_path.join(".s3uploads");
    let marker_path = uploads_dir.join(format!("{}.json", params.uploadId));
    if get(&marker_path, "s3.upload").await?.is_none() {
        return Err(AppError::NoSuchUpload);
    }

    let parts_dir = uploads_dir.join(&params.uploadId);

    // ────────────────────────────────
    // SECTION 2: Cleanup parts + marker
    // ────────────────────────────────
    let _ = fs::remove_dir_all(&parts_dir).await;
    let _ = fs::remove_file(&marker_path).await;

    // WAL tombstone entry for aborted upload (optional)
    let idx = state.get_bucket_index(&bucket).await
        .map_err(|e| AppError::Internal(anyhow::anyhow!(format!("open index: {e}"))))?;
    idx.put(IndexEntry {
        key: key.clone(),
        size: 0,
        etag: "".into(),
        last_modified: s3_now(),
        seq_no: 0,
        is_delete: true,
        version: Some(params.uploadId.clone()),
    }).await.map_err(|e| AppError::Internal(anyhow::anyhow!(format!("index tombstone: {e}"))))?;

    // ────────────────────────────────
    // SECTION 3: Response
    // ────────────────────────────────
    let mut headers = S3Headers::common_headers();
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    Ok((StatusCode::NO_CONTENT, headers))
}