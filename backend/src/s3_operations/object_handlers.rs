use axum::{
    extract::{Path as AxumPath, State, Query},
    response::{Response, IntoResponse},
    body::Body,
    http::{StatusCode, header, HeaderMap,HeaderValue},
    extract::Request,Extension
};
use std::{path::{Path, PathBuf},mem};
use tokio::{fs::{File, self}, sync::{Semaphore, Mutex}, io::{AsyncWriteExt,AsyncReadExt, AsyncSeekExt, BufWriter}};
use tokio_util::io::ReaderStream;
use std::io;
use futures_util::StreamExt;
use std::collections::VecDeque;
use anyhow::Context;
use chrono::{DateTime, Utc};
use md5;
use hex;
use tracing::info;
use uuid::Uuid;
use crate::{
    s3_operations::{handler_utils::{AppError,ObjectInfo, CommonPrefix}, auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel},metadata::{get, set},bucket_handlers::BucketMeta},
    AppState,S3_XMLNS, S3Headers,GLOBAL_IO_SEMAPHORE,
};
use base64::Engine;
use serde::{Serialize, Deserialize,};
use http_range::HttpRange;
use httpdate::fmt_http_date;
use std::sync::atomic::{AtomicU64, Ordering};
use md5::Context as Md5Context;

use once_cell::sync::Lazy;
use bytes::Bytes;
use std::{io::ErrorKind, time::{SystemTime,Instant}, collections::{HashSet,HashMap}, sync::Arc,convert::TryInto};
use sha2::{Digest, Sha256};
use base64::engine::general_purpose::STANDARD as BASE64;

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

#[derive(Debug, Serialize, Deserialize, Default)]
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

//Indexing
#[derive(Debug, Serialize, Deserialize)]
struct IndexEntry {
    key: String,
    last_modified: String,
    size: u64,
    etag: String,
}


// async fn load_index(bucket_root: &Path, prefix: &str) -> io::Result<Vec<IndexEntry>> {
//     let idx_path = bucket_root.join(".s3index").join(prefix);
//     match get(&idx_path, "s3.index").await {
//         Ok(Some(data)) => {
//             let entries: Vec<IndexEntry> = serde_json::from_slice(&data)?;
//             Ok(entries)
//         }
//         Ok(None) => Ok(Vec::new()),
//         Err(e) => Err(e),
//     }
// }





// async fn save_index(bucket_root: &Path, prefix: &str, entries: &[IndexEntry]) -> io::Result<()> {
//     let idx_dir = bucket_root.join(".s3index");
//     fs::create_dir_all(&idx_dir).await?;
//     let idx_path = idx_dir.join(prefix);
//     let data = serde_json::to_vec(entries)?;
//     set(&idx_path, "s3.index", &data).await
// }


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


pub async fn put_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: Extension<AuthenticatedUser>,
    req: axum::extract::Request,
) -> Result<(StatusCode, HeaderMap), AppError> {
    PUT_COUNT.fetch_add(1, Ordering::Relaxed);
    let start = std::time::Instant::now();

    let req_id = format!("put-{}-{}", bucket, key);
    tracing::info!(req_id, bucket=%bucket, key=%key, "PUT begin");

    // 1) AuthZ
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

    // 2) Concurrency controls
    let _permit = state.io_budget.acquire().await.unwrap();
    let lock_key = format!("{bucket}/{key}");
    let _guard = state.io_locks.lock(lock_key).await;

    // 3) Paths
    let base_path = state.object_path(&bucket, &key);
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }
    let parent = base_path.parent().unwrap().to_path_buf();
    fs::create_dir_all(&parent).await.context("create parent").map_err(AppError::Internal)?;

    // 4) Bucket meta
    let bucket_meta_path = bucket_path.join(".s3meta");
    let bucket_meta_json = get(&bucket_meta_path, "user.s3.meta")
        .await
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .unwrap_or_else(|| r#"{"versioning":false,"object_count":0,"used_bytes":0}"#.to_string());
    let mut bucket_meta: BucketMeta = serde_json::from_str(&bucket_meta_json)
        .unwrap_or_else(|_| BucketMeta {
            versioning: false,
            object_count: 0,
            used_bytes: 0,
            ..Default::default()
        });
    let versioning = bucket_meta.versioning;

    // 5) Object meta sidecar
    let safe_key = key.replace('/', "_");
    let meta_marker = parent.join(format!(".s3meta.{safe_key}"));
    let mut obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
        .await
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

    // 6) Conditional headers
    if let Some(if_match) = req.headers().get(header::IF_MATCH).and_then(|h| h.to_str().ok()) {
        let expected = if_match.trim();
        if !expected.is_empty() && obj_meta.etag != expected {
            return Err(AppError::PreconditionFailed);
        }
    }
    if let Some(if_none_match) = req.headers().get(header::IF_NONE_MATCH).and_then(|h| h.to_str().ok()) {
        let forbidden = if_none_match.trim();
        if !forbidden.is_empty() && obj_meta.etag == forbidden {
            return Err(AppError::PreconditionFailed);
        }
    }

        // 7) Version ID (opaque)
    let new_version_id = if versioning {
        Some(Uuid::new_v4().to_string())
    } else {
        None
    };

    // Always write to canonical path
    let final_path = base_path.clone();

    // 8) Content headers + limits + integrity inputs
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let content_len = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    if let Some(len) = content_len {
        let max_bytes = std::env::var("MAX_OBJECT_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(u64::MAX);
        if len > max_bytes {
            return Err(AppError::EntityTooLarge);
        }
    }

    let content_md5_b64 = req
        .headers()
        .get("Content-MD5")
        .and_then(|h| h.to_str().ok())
        .map(str::to_string);

    let x_amz_sha256_hex = req
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|h| h.to_str().ok())
        .map(str::to_string);

    let mut user_meta = std::collections::HashMap::new();
    for (name, value) in req.headers().iter() {
        if let Some(rest) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(vs) = value.to_str() {
                user_meta.insert(rest.to_string(), vs.to_string());
            }
        }
    }

    let tags = req
        .headers()
        .get("x-amz-tagging")
        .and_then(|h| h.to_str().ok())
        .map(parse_tagging)
        .unwrap_or_default();

    // Capture old size (for bucket stats adjustment on overwrite)
    let old_size = match fs::metadata(&final_path).await {
        Ok(m) => m.len(),
        Err(_) => 0,
    };

    // Determine if this is a new object (S3-style: based on existence)
    let is_new_object = old_size == 0;

    // 9) Temp write with buffered IO, prealloc, fsync
    let tmp_path = final_path.with_extension("upload.tmp");

    struct TempGuard {
        path: PathBuf,
        committed: bool,
    }

    impl Drop for TempGuard {
        fn drop(&mut self) {
            if !self.committed {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }

    let mut guard = TempGuard {
        path: tmp_path.clone(),
        committed: false,
    };

    let file = fs::File::create(&tmp_path)
        .await
        .context("create temp")
        .map_err(AppError::Internal)?;
    let mut writer = BufWriter::with_capacity(512 * 1024, file);
    if let Some(len) = content_len {
        let _ = writer.get_mut().set_len(len).await;
    }

    let mut md5_hasher = Md5Context::new();
    let mut sha256_hasher = Sha256::new();
    let mut total_size = 0u64;
    let mut body_stream = req.into_body().into_data_stream();

    while let Some(chunk) = body_stream.next().await {
        let chunk = chunk.context("read chunk").map_err(AppError::Internal)?;
        let bytes = chunk.as_ref();
        md5_hasher.consume(bytes);
        sha256_hasher.update(bytes);
        writer
            .write_all(bytes)
            .await
            .context("write chunk")
            .map_err(AppError::Internal)?;
        total_size += bytes.len() as u64;
    }

    writer
        .flush()
        .await
        .context("flush")
        .map_err(AppError::Internal)?;
    writer
        .get_mut()
        .sync_data()
        .await
        .context("sync_data")
        .map_err(AppError::Internal)?;
    writer
        .get_mut()
        .sync_all()
        .await
        .context("sync_all")
        .map_err(AppError::Internal)?;

    // 10) ETag + checksum verification
    let md5_digest = md5_hasher.finalize();
    let etag = format!("\"{}\"", hex::encode(md5_digest.0));

    if let Some(md5_b64) = content_md5_b64 {
        let computed_b64 = BASE64.encode(md5_digest.0);
        if computed_b64 != md5_b64 {
            return Err(AppError::PreconditionFailed);
        }
    }

    if let Some(sha256_hex) = x_amz_sha256_hex {
        if sha256_hex != "UNSIGNED-PAYLOAD" {
            let computed_hex = hex::encode(sha256_hasher.finalize());
            if computed_hex != sha256_hex {
                return Err(AppError::PreconditionFailed);
            }
        }
    }

    // 11) Archive previous version (before overwrite), then atomic commit
    if versioning && old_size > 0 {
        let archive_dir = parent.join(".s3versions").join(&safe_key);
        let prev_version_id = obj_meta
            .version_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let archive_path = archive_dir.join(prev_version_id);
        fs::create_dir_all(&archive_dir).await.ok();

        // Try hard link first (fast, space-efficient), fallback to copy
        match fs::hard_link(&final_path, &archive_path).await {
            Ok(_) => {}
            Err(_) => {
                let _ = fs::copy(&final_path, &archive_path).await;
            }
        }
    }

    // Atomic commit: rename temp -> canonical path
    fs::rename(&tmp_path, &final_path)
        .await
        .context("rename temp->final")
        .map_err(AppError::Internal)?;
    guard.committed = true;

    // 12) Update object metadata
    obj_meta.etag = etag.clone();
    obj_meta.content_type = content_type;
    obj_meta.owner = user.0.username.clone();
    obj_meta.is_delete_marker = false;
    obj_meta.user_meta = user_meta;
    obj_meta.tags = tags;

    // S3-compatible: use opaque UUID version IDs only; no numeric current_version
    obj_meta.version_id = new_version_id.clone();


    let obj_json = serde_json::to_string_pretty(&obj_meta)
        .context("serialize obj meta")
        .map_err(AppError::Internal)?;

    // --- xattr safety: ensure file exists before set ---
    #[cfg(unix)]
    {
        if fs::metadata(&meta_marker).await.is_err() {
            let _ = fs::File::create(&meta_marker).await;
        }
    }

    set(&meta_marker, "user.s3.meta", obj_json.as_bytes())
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("set obj meta: {e}")))?;

    // 13) Update bucket stats
    if is_new_object {
        bucket_meta.object_count = bucket_meta.object_count.saturating_add(1);
    }
    if versioning {
        bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(total_size);
    } else {
        let delta = if total_size >= old_size { total_size - old_size } else { 0 };
        bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(delta);
    }

    let bucket_json = serde_json::to_string_pretty(&bucket_meta)
        .context("serialize bucket meta")
        .map_err(AppError::Internal)?;

    // --- xattr safety for bucket meta ---
    #[cfg(unix)]
    {
        if fs::metadata(&bucket_meta_path).await.is_err() {
            let _ = fs::File::create(&bucket_meta_path).await;
        }
    }

    set(&bucket_meta_path, "user.s3.meta", bucket_json.as_bytes())
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("set bucket meta: {e}")))?;

    // 13b) Update index for prefix
    use chrono::{DateTime, Utc};
    let last_modified = DateTime::<Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    let prefix = parent.strip_prefix(&bucket_path)
        .ok()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    let idx_path = bucket_path.join(".s3index").join(&prefix);

    #[cfg(unix)]
    {
        if fs::metadata(&idx_path).await.is_err() {
            let _ = fs::File::create(&idx_path).await;
        }
    }

    let mut entries: Vec<IndexEntry> = match get(&idx_path, "s3.index").await {
        Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or_default(),
        _ => Vec::new(),
    };

    if let Some(existing) = entries.iter_mut().find(|e| e.key == key) {
        existing.last_modified = last_modified.clone();
        existing.size = total_size;
        existing.etag = etag.clone();
    } else {
        entries.push(IndexEntry {
            key: key.clone(),
            last_modified,
            size: total_size,
            etag: etag.clone(),
        });
    }
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    let data = serde_json::to_vec(&entries).map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
    set(&idx_path, "s3.index", &data)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("set index: {e}")))?;

    // 14) Metrics + trace
    PUT_BYTES.fetch_add(total_size, Ordering::Relaxed);
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

    // 15) Response
    let mut headers = S3Headers::common_headers();
    headers.insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    if let Some(v) = new_version_id {
        headers.insert("x-amz-version-id", HeaderValue::from_str(&v).unwrap());
    }
    headers.insert("x-amz-request-id", HeaderValue::from_str(&req_id).unwrap());

    Ok((StatusCode::OK, headers))
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
    // 0. Global IO permit (limit concurrent heavy IO)
    let _io_permit = GLOBAL_IO_SEMAPHORE.acquire().await.unwrap();

    info!("GET {}/{}?{:?}", bucket, key, params);

    // 1. Permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(AppError::Internal)?
    {
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

    // 4. Bucket versioning flag (same pattern as PUT)
    let versioning = get(&bucket_path.join(".s3meta"), "user.s3.meta")
        .await
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v.get("versioning")?.as_bool())
        .unwrap_or(false);

    // 5. Load object metadata to decide latest/delete-marker/version
    let obj_meta = load_object_meta(&parent, &safe_key).await?;
    let req_version = params.get("versionId").cloned();

    // 6. Version resolution
    // - If versioning ON and latest is a delete marker:
    //   - No versionId → 404
    //   - versionId == latest delete marker → 404
    // - If versioning ON and versionId provided:
    //   - If versionId == latest non-delete → serve canonical file
    //   - Else → serve archived file under .s3versions/{safe_key}/{versionId}
    // - If versioning OFF → serve canonical file or 404 if missing
    let object_path = if versioning {
        match (&req_version, obj_meta.version_id.clone(), obj_meta.is_delete_marker) {
            (None, _, true) => return Err(AppError::NoSuchKey), // latest is delete marker
            (Some(vid), Some(latest_vid), true) if &latest_vid == vid => return Err(AppError::NoSuchKey),
            (Some(vid), Some(latest_vid), false) if &latest_vid == vid => full_key_path.clone(),
            (Some(vid), _, _) => {
                // archived version
                let archive_path = parent.join(".s3versions").join(&safe_key).join(vid);
                if !archive_path.exists() {
                    return Err(AppError::NotFound(format!("version {}", vid)));
                }
                archive_path
            }
            // No versionId, latest is not a delete marker
            (None, _, false) => full_key_path.clone(),
        }
    } else {
        full_key_path.clone()
    };

    // 7. Open file
    let mut file = File::open(&object_path)
        .await
        .map_err(|e| if e.kind() == std::io::ErrorKind::NotFound {
            AppError::NoSuchKey
        } else {
            AppError::Io(e)
        })?;
    let file_meta = file.metadata().await.context("file metadata")?;
    let total_size = file_meta.len();

    // 8. Conditional headers support (ETag-based)
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

    // 9. Range parsing
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

    // 10. Platform safety: reject if > usize::MAX
    if total_size > usize::MAX as u64 {
        return Err(AppError::Internal(anyhow::anyhow!(
            "File too large for platform (max {} bytes)",
            usize::MAX
        )));
    }

    // 11. sendfile fast path (UNIX only, full file, no range)
    let use_sendfile = cfg!(unix) && !is_partial && content_length == total_size;
    if use_sendfile {
        // Build headers
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
        let mut response = resp.body(Body::empty()).map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        if try_sendfile(&mut file, 0, total_size, &mut response).await? {
            info!("sendfile served {object_path:?} | {total_size} bytes | ETag: {}", obj_meta.etag);
            return Ok(response);
        }
        // fall through if sendfile failed
    }

    // 12. Linux readahead hint (sequential)
    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::{posix_fadvise, PosixFadviseAdvice};
        let fd = file.as_raw_fd();
        let _ = posix_fadvise(fd, 0, 0, PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL);
    }

    // 13. Seek for range
    if is_partial {
        file.seek(std::io::SeekFrom::Start(start)).await.context("seek")?;
    }

    // 14. 32/64-bit safe streaming via ReaderStream.take(limit)
    const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
    let chunk_size_u64 = CHUNK_SIZE as u64;
    let num_chunks_u64 = if content_length == 0 {
        0
    } else {
        (content_length + chunk_size_u64 - 1) / chunk_size_u64
    };

    let (body, response_content_length): (Body, u64) = if mem::size_of::<usize>() == 8 {
        // 64-bit
        let stream = ReaderStream::with_capacity(file, CHUNK_SIZE).take(num_chunks_u64 as usize);
        (Body::from_stream(stream), content_length)
    } else {
        // 32-bit
        const MAX_32BIT_SAFE: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB guidance
        if content_length > MAX_32BIT_SAFE {
            let stream = ReaderStream::with_capacity(file, CHUNK_SIZE);
            (Body::from_stream(stream), content_length)
        } else {
            let limit: usize = num_chunks_u64
                .try_into()
                .map_err(|_| AppError::Internal(anyhow::anyhow!("Chunk count exceeds usize::MAX on 32-bit")))?;
            let stream = ReaderStream::with_capacity(file, CHUNK_SIZE).take(limit);
            (Body::from_stream(stream), content_length)
        }
    };

    // 15. Headers
    let mut resp_headers = S3Headers::common_headers();
    resp_headers.insert(axum::http::header::CONTENT_TYPE, obj_meta.content_type.parse().unwrap());
    resp_headers.insert(axum::http::header::ACCEPT_RANGES, "bytes".parse().unwrap());
    resp_headers.insert(axum::http::header::ETAG, obj_meta.etag.parse().unwrap());
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
    resp_headers.insert(axum::http::header::CONTENT_LENGTH, response_content_length.to_string().parse().unwrap());

    let status = if is_partial { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };

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







/// Walk the bucket, skip every `.s3meta*` file and return:
/// * `files` – (relative Path, mtime, size)
/// * `dirs`  – (relative Path) of every directory
pub async fn traverse_and_get_keys(
    root: &Path,
    base: &Path,
    files: &mut Vec<(PathBuf, SystemTime, u64)>,
    dirs: &mut Vec<String>,
) -> Result<(), AppError> {
    use std::collections::VecDeque;
    let mut stack = VecDeque::from([root.to_path_buf()]);

    while let Some(dir) = stack.pop_back() {
        let mut entries = fs::read_dir(&dir).await.context("read dir")?;

        while let Some(entry) = entries.next_entry().await.context("next entry")? {
            let path = entry.path();
            let meta = fs::symlink_metadata(&path).await.context("metadata")?;
            if meta.is_symlink() {
                continue; // skip symlinks to avoid recursion loops
            }

            // Skip hidden/system metadata and version directories
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name == ".s3meta"
                    || name == ".s3meta.json"
                    || name.starts_with(".s3meta.")
                    || name == ".s3versions"
                    || name == ".s3index"
                {
                    continue;
                }
            }

            if meta.is_file() {
                if let Ok(rel) = path.strip_prefix(base) {
                    files.push((
                        rel.to_path_buf(),
                        meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                        meta.len(),
                    ));
                }
            } else if meta.is_dir() {
                if let Ok(rel) = path.strip_prefix(base) {
                    dirs.push(format!("{}/", rel.to_string_lossy().replace('\\', "/")));
                }
                stack.push_back(path);
            }
        }
    }
    Ok(())
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

    // --- Load index ---
    let idx_path = root.join(".s3index").join(&prefix);
    let mut entries: Vec<IndexEntry> = match get(&idx_path, "s3.index").await {
        Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or_default(),
        _ => {
            // fallback: scan filesystem if index missing
            let mut files = Vec::new();
            let mut dirs = Vec::new();
            traverse_and_get_keys(&root, &root, &mut files, &mut dirs).await?;
            files.sort_by_key(|f| f.0.clone());
            files.into_iter().map(|(path, mtime, size)| {
                IndexEntry {
                    key: path.to_string_lossy().replace('\\', "/"),
                    last_modified: DateTime::<Utc>::from(mtime)
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    size,
                    etag: format!("\"{}\"", hex::encode(md5::compute(format!("{}{}", path.display(), size).as_bytes()).0)),
                }
            }).collect()
        }
    };

    // --- Filter + paginate ---
    let mut objects = Vec::new();
    let mut prefixes = std::collections::HashSet::new();
    let mut next_token: Option<String> = None;
    let mut emitted = 0usize;
    let mut last_key_emitted: Option<String> = None;

    for entry in entries.into_iter().filter(|e| e.key > start_after_key) {
        if !prefix.is_empty() && !entry.key.starts_with(&prefix) {
            continue;
        }

        // delimiter handling
        if !delimiter.is_empty() {
            if let Some(rest) = entry.key.strip_prefix(&prefix) {
                if let Some(pos) = rest.find(&delimiter) {
                    prefixes.insert(format!("{}{}", prefix, &rest[..pos + delimiter.len()]));
                    continue;
                }
            }
        }

        objects.push(ObjectInfo {
            key: entry.key.clone(),
            last_modified: entry.last_modified.clone(),
            size_bytes: entry.size,
            etag: entry.etag.clone(),
        });

        emitted += 1;
        last_key_emitted = Some(entry.key.clone());

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

    // Build XML response
    // let result = ListBucketResult {
    //     xmlns: S3_XMLNS,
    //     bucket_name: bucket,
    //     prefix: (!prefix.is_empty()).then_some(prefix),
    //     delimiter: (!delimiter.is_empty()).then_some(delimiter),
    //     start_after: (!query.start_after.clone().unwrap_or_default().is_empty())
    //         .then_some(query.start_after.clone().unwrap()),
    //     continuation_token: query.continuation_token.clone(),
    //     next_continuation_token: next_token.clone(),
    //     max_keys: max_keys_s3,
    //     is_truncated,
    //     objects,
    //     common_prefixes: prefixes.into_iter().map(|p| CommonPrefix { prefix: p }).collect(),
    // };
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



// pub async fn list_objects(
//     State(state): State<Arc<AppState>>,
//     AxumPath(bucket): AxumPath<String>,
//     Query(query): Query<ListObjectsQuery>,
//     user: axum::Extension<AuthenticatedUser>,
// ) -> Result<(HeaderMap, String), AppError> {
//     tracing::info!(
//         "LIST Request: Bucket='{}', Query={:?}, User='{}'",
//         bucket,
//         query,
//         user.0.username
//     );

//     // ---- 1. Permission ----------------------------------------------------
//     if !check_bucket_permission(
//         &state.pool,
//         &user.0,
//         &bucket,
//         PermissionLevel::ReadOnly.as_str(),
//     )
//     .await
//     .map_err(AppError::Internal)?
//     {
//         return Err(AppError::AccessDenied);
//     }

//     let bucket_root = state.bucket_path(&bucket);
//     if !bucket_root.exists() {
//         return Err(AppError::NotFound(bucket));
//     }

//     // ---- 2. Query parameters (S3 uses u32) -------------------------------
//     let prefix = query.prefix.unwrap_or_default();
//     let delimiter = query.delimiter.unwrap_or_default();
//     let marker = query.marker.unwrap_or_default();

//     // S3: max-keys is u32, cap at 1000
//     let max_keys_s3: u32 = query.max_keys.unwrap_or(1000).min(1000);
//     let max_keys: usize = max_keys_s3 as usize; // ← 100% safe

//     // ---- 3. Gather every *real* file (skip .s3meta.*) --------------------
//     let mut all_keys_metadata = Vec::<(PathBuf, SystemTime, u64)>::new();
//     traverse_and_get_keys(&bucket_root, &bucket_root, &mut all_keys_metadata).await?;
//     all_keys_metadata.sort_by(|a, b| a.0.cmp(&b.0));

//     // ---- 4. Filter & apply S3 semantics ---------------------------------
//     let mut objects = Vec::<ObjectInfo>::new();
//     let mut common_prefixes = HashSet::<String>::new();
//     let mut processed = 0usize;
//     let mut is_truncated = false;

//     for (rel_path, modified, size) in all_keys_metadata {
//         let key = rel_path.to_string_lossy().replace('\\', "/");

//         // Skip marker
//         if !marker.is_empty() && key <= marker {
//             continue;
//         }

//         // Skip prefix
//         if !prefix.is_empty() && !key.starts_with(&prefix) {
//             continue;
//         }

//         // Delimiter → CommonPrefixes
//         if !delimiter.is_empty() {
//             if let Some(rest) = key.strip_prefix(&prefix) {
//                 if let Some(pos) = rest.find(&delimiter) {
//                     let cp = format!("{}{}", prefix, &rest[..pos + delimiter.len()]);
//                     common_prefixes.insert(cp);
//                     continue;
//                 }
//             }
//         }

//         // Max keys
//         if processed >= max_keys {
//             is_truncated = true;
//             break;
//         }

//         // Skip bucket metadata files
//         if let Some(name) = rel_path.file_name().and_then(|n| n.to_str()) {
//             if name == ".s3meta" || name == ".s3meta.json" {
//                 continue;
//             }
//         }

//         // Load object metadata
//         let safe_key = key.replace('/', "_");
//         let meta_path = bucket_root.join(&rel_path).parent().unwrap().join(format!(".s3meta.{}", safe_key));

//         let obj_meta = match get(&meta_path, "user.s3.meta")
//             .ok()
//             .flatten()
//             .and_then(|b| String::from_utf8(b).ok())
//             .and_then(|s| serde_json::from_str::<ObjectMeta>(&s).ok())
//         {
//             Some(meta) => meta,
//             None => ObjectMeta {
//                 etag: format!("\"{}\"", hex::encode(md5::compute(format!("{}{}", key, size).as_bytes()).0)),
//                 content_type: "application/octet-stream".to_string(),
//                 ..Default::default()
//             },
//         };

//         let last_modified = DateTime::<Utc>::from(modified)
//             .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

//         objects.push(ObjectInfo {
//             key,
//             last_modified,
//             size_bytes: size,
//             etag: obj_meta.etag,
//         });

//         processed += 1;
//     }

//     // ---- 5. Build XML (S3 expects u32) -----------------------------------
//     let result = ListBucketResult {
//         xmlns: S3_XMLNS,
//         bucket_name: bucket.clone(),
//         prefix: (!prefix.is_empty()).then_some(prefix),
//         delimiter: (!delimiter.is_empty()).then_some(delimiter),
//         marker: (!marker.is_empty()).then_some(marker),
//         max_keys: max_keys_s3,  // ← u32
//         is_truncated,
//         objects,
//         common_prefixes: common_prefixes
//             .into_iter()
//             .map(|p| CommonPrefix { prefix: p })
//             .collect(),
//     };

//     let xml_body = quick_xml::se::to_string(&result)
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("XML serialization: {e}")))?;

//     let mut headers = S3Headers::xml_headers();
//     headers.insert(
//         header::CONTENT_LENGTH,
//         HeaderValue::from_str(&xml_body.len().to_string())
//             .expect("content-length is valid"),
//     );

//     Ok((headers, xml_body))
// }







// S3 DELETE Object Operation
// S3 DELETE Object Operation
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
        // Instead of deleting, mark delete marker
        version_id = Some(Uuid::new_v4().to_string());
        obj_meta.is_delete_marker = true;
        obj_meta.version_id = version_id.clone();

        let obj_json = serde_json::to_string_pretty(&obj_meta)
            .map_err(|e| AppError::Internal(anyhow::Error::new(e)))?;
        #[cfg(unix)]
        {
            if fs::metadata(&meta_marker).await.is_err() {
                let _ = fs::File::create(&meta_marker).await;
            }
        }
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
    #[cfg(unix)]
    {
        if fs::metadata(&bucket_meta_path).await.is_err() {
            let _ = fs::File::create(&bucket_meta_path).await;
        }
    }
    set(&bucket_meta_path, "user.s3.meta", bucket_json.as_bytes()).await?;

    // 7) Update index (prune key)
    let prefix = parent.strip_prefix(&bucket_path)
        .ok()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();
    let idx_path = bucket_path.join(".s3index").join(&prefix);
    #[cfg(unix)]
    {
        if fs::metadata(&idx_path).await.is_err() {
            let _ = fs::File::create(&idx_path).await;
        }
    }
    let mut entries: Vec<IndexEntry> = match get(&idx_path, "s3.index").await {
        Ok(Some(data)) => serde_json::from_slice(&data).unwrap_or_default(),
        _ => Vec::new(),
    };
    entries.retain(|e| e.key != key);
    let data = serde_json::to_vec(&entries)
        .map_err(|e| AppError::Internal(anyhow::Error::new(e)))?;
    set(&idx_path, "s3.index", &data).await?;

    // 8) Response
    let mut headers = S3Headers::common_headers();
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    if let Some(v) = version_id {
        headers.insert("x-amz-version-id", HeaderValue::from_str(&v).unwrap());
    }
    headers.insert("x-amz-request-id", HeaderValue::from_str(&format!("del-{}-{}", bucket, key)).unwrap());

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



//DELETE WITH MARKER
// pub async fn delete_object(
//     State(state): State<Arc<AppState>>,
//     AxumPath((bucket, key)): AxumPath<(String, String)>,
//     user: axum::Extension<AuthenticatedUser>,
// ) -> Result<(StatusCode, HeaderMap), AppError> {
//     tracing::info!("DELETE {}/{}", bucket, key);

//     // 1. Permission
//     check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
//         .await
//         .map_err(AppError::Internal)?
//         .then_some(())
//         .ok_or(AppError::AccessDenied)?;

//     let bucket_path = state.bucket_path(&bucket);
//     if !bucket_path.exists() {
//         return Err(AppError::NotFound(bucket));
//     }

//     let parent = state.object_path(&bucket, &key).parent().unwrap().to_path_buf();
//     let safe_key = key.replace('/', "_");
//     let meta_marker = parent.join(format!(".s3meta.{}", safe_key));

//     // 2. Load bucket meta
//     let bucket_meta_path = bucket_path.join(".s3meta");
//     let bucket_meta = load_bucket_meta(&bucket_meta_path).await?;

//     let versioning = bucket_meta.versioning;

//     // 3. Load object meta
//     let mut obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
//         .ok()
//         .flatten()
//         .and_then(|b| String::from_utf8(b).ok())
//         .and_then(|s| serde_json::from_str(&s).ok())
//         .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

//     // 4. If versioning → create delete marker
//     if versioning {
//         let next_version = obj_meta.current_version + 1;
//         let marker_path = parent.join(format!("{}.v{}", key, next_version));

//         // Create empty file (delete marker)
//         fs::write(&marker_path, b"").await.context("create delete marker")?;

//         // Update metadata
//         obj_meta.current_version = next_version;
//         obj_meta.version_id = Some(next_version);
//         obj_meta.is_delete_marker = true;
//         obj_meta.etag = "\"delete-marker\"".to_string();
//         obj_meta.content_type = "application/octet-stream".to_string();

//         let json = serde_json::to_string_pretty(&obj_meta).context("serialize obj meta")?;
//         set(&meta_marker, "user.s3.meta", json.as_bytes())
//             .map_err(|e| AppError::Internal(anyhow::anyhow!("save meta: {e}")))?;

//         // Update bucket stats
//         let mut bucket_meta = bucket_meta;
//         bucket_meta.used_bytes += 0; // empty file
//         save_bucket_meta(&bucket_meta_path, &bucket_meta).await?;

//         let mut headers = S3Headers::common_headers();
//         headers.insert("x-amz-version-id", next_version.to_string().parse().unwrap());
//         headers.insert("x-amz-delete-marker", "true".parse().unwrap());

//         return Ok((StatusCode::NO_CONTENT, headers));
//     }

//     // 5. Non-versioned: actually delete
//     let base_path = state.object_path(&bucket, &key);
//     if base_path.exists() {
//         fs::remove_file(&base_path).await.context("delete file")?;
//     }

//     // Remove metadata
//     if meta_marker.exists() {
//         fs::remove_file(&meta_marker).await.ok();
//     }

//     // Update bucket stats
//     let size = if base_path.exists() { fs::metadata(&base_path).await?.len() } else { 0 };
//     let mut bucket_meta = bucket_meta;
//     bucket_meta.object_count = bucket_meta.object_count.saturating_sub(1);
//     bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_sub(size);
//     save_bucket_meta(&bucket_meta_path, &bucket_meta).await?;

//     Ok((StatusCode::NO_CONTENT, HeaderMap::new()))
// }