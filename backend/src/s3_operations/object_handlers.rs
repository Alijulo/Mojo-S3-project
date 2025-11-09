use axum::{
    extract::{Path as AxumPath, State, Query},
    response::{Response, IntoResponse},
    body::Body,
    http::{StatusCode, header, HeaderMap,HeaderValue},
    extract::Request,
};
use std::path::{Path, PathBuf};
use tokio::{fs::{File, self}, io::{AsyncWriteExt}};
use tokio_util::io::ReaderStream;
use futures_util::StreamExt;
use std::{io::ErrorKind, time::SystemTime, collections::HashSet, sync::Arc};
use anyhow::Context;
use chrono::{DateTime, Utc};
use md5;
use hex;
use crate::{
    s3_operations::{handler_utils, auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel},metadata::{get, set},bucket_handlers::BucketMeta},
    AppError, AppState, ListBucketResult, ObjectInfo, CommonPrefix,
    S3_XMLNS, to_xml_string, S3Headers,
};
use serde::{Serialize, Deserialize,};
use http_range::HttpRange;
use httpdate::fmt_http_date;
use tokio::io::AsyncSeekExt;

/// S3 List Objects Query Parameters
#[derive(Debug, Deserialize)]
pub struct ListObjectsQuery {
    #[serde(rename = "prefix")]
    pub prefix: Option<String>,
    #[serde(rename = "delimiter")]
    pub delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    pub max_keys: Option<u32>,
    #[serde(rename = "marker")]
    pub marker: Option<String>,
}




#[derive(Debug, Serialize, Deserialize, Default)]
struct ObjectMeta {
    #[serde(default)]
    etag: String,
    #[serde(default)] 
    content_type: String,
    #[serde(default)] 
    owner: String,
    #[serde(default)] 
    current_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")] 
    version_id: Option<u64>,
}

impl ObjectMeta {
    fn new(owner: &str) -> Self {
        Self {
            owner: owner.to_string(),
            ..Default::default()
        }
    }
}

// // S3 PUT Object Operation (Upload)
pub async fn put_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: axum::Extension<AuthenticatedUser>,
    req: Request,
) -> Result<(StatusCode, HeaderMap), AppError> {
    tracing::info!("PUT {}/{}", bucket, key);

    // 1. Permission
    check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(AppError::Internal)?
        .then_some(())
        .ok_or(AppError::AccessDenied)?;

    let base_path = state.object_path(&bucket, &key);
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() { return Err(AppError::NotFound(bucket)); }

    let parent = base_path.parent().unwrap();
    fs::create_dir_all(parent).await.context("create parent")?;

    // 2. Load bucket metadata (for versioning + stats)
    let bucket_meta_path = bucket_path.join(".s3meta");
    let bucket_meta_json = get(&bucket_meta_path, "user.s3.meta")
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

    // 3. Shared object metadata marker
    let safe_key = key.replace('/', "_");
    let meta_marker = parent.join(format!(".s3meta.{}", safe_key));

    #[cfg(unix)]
    if fs::metadata(&meta_marker).await.is_err() {
        fs::write(&meta_marker, b"").await.map_err(AppError::Io)?;
    }

    // 4. Load object metadata
    let mut obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

    // 5. Determine object path and version
    let (object_path, version_id, is_new_version) = if versioning {
        let v = obj_meta.current_version + 1;
        (parent.join(format!("{}.v{}", key, v)), Some(v), v == 1)
    } else {
        (base_path.clone(), None, obj_meta.current_version == 0)
    };

    // 6. Upload + ETag
    let content_type = req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let mut hasher = md5::Context::new();
    let mut file = fs::File::create(&object_path).await.context("create file")?;
    let mut stream = req.into_body().into_data_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read chunk")?;
        hasher.consume(&chunk);
        file.write_all(&chunk).await.context("write chunk")?;
    }
    file.flush().await.context("flush")?;

    let size = fs::metadata(&object_path).await?.len();
    let etag = format!("\"{}\"", hex::encode(hasher.finalize().0));

    // 7. Update object metadata
    obj_meta.etag = etag.clone();
    obj_meta.content_type = content_type;
    if let Some(v) = version_id {
        obj_meta.current_version = v;
        obj_meta.version_id = Some(v);
    }

    let obj_json = serde_json::to_string_pretty(&obj_meta).context("serialize obj meta")?;
    set(&meta_marker, "user.s3.meta", obj_json.as_bytes())
        .map_err(|e| AppError::Internal(anyhow::anyhow!("write obj meta: {e}")))?;

    // 8. Update bucket stats — ONLY on new object/version
    if is_new_version {
        bucket_meta.object_count += 1;
    }
    bucket_meta.used_bytes += size;

    let bucket_json = serde_json::to_string_pretty(&bucket_meta).context("serialize bucket meta")?;
    set(&bucket_meta_path, "user.s3.meta", bucket_json.as_bytes())
        .map_err(|e| AppError::Internal(anyhow::anyhow!("write bucket meta: {e}")))?;

    // 9. Respond
    tracing::info!(
        "Uploaded {} | ETag: {} | Size: {} | v{:?} | Bucket: {} objs, {} bytes",
        object_path.display(),
        etag,
        size,
        version_id,
        bucket_meta.object_count,
        bucket_meta.used_bytes
    );

    let mut headers = S3Headers::common_headers();
    headers.insert(header::ETAG, etag.parse().unwrap());
    if let Some(v) = version_id {
        headers.insert("x-amz-version-id", v.to_string().parse().unwrap());
    }

    Ok((StatusCode::OK, headers))
}

/// Helper function to recursively traverse a directory and collect all file paths
/// relative to the base_path (the bucket root), while IGNORING metadata files.
async fn traverse_and_get_keys(
    current_path: &Path,
    base_path: &Path,
    all_keys: &mut Vec<(PathBuf, SystemTime, u64)>, // (Relative Key, Modified Time, Size)
) -> Result<(), AppError> {
    let mut entries = fs::read_dir(current_path).await.context("Failed to read directory")?;

    while let Some(entry) = entries.next_entry().await.context("Failed to read directory entry")? {
        let path = entry.path();
        let metadata = match fs::symlink_metadata(&path).await {
            Ok(meta) => meta,
            Err(_) => continue, // Skip entries with inaccessible metadata
        };

        // Skip symbolic links to avoid infinite recursion
        if metadata.is_symlink() {
            tracing::warn!("Skipping symbolic link: {}", path.display());
            continue;
        }

        if metadata.is_file() {
            // Skip metadata files
            if let Some(file_name) = path.file_name().and_then(|name| name.to_str()) {
                if file_name.ends_with(".metadata.json") {
                    continue;
                }
            }

            // Calculate the key relative to the bucket root
            if let Ok(relative_path) = path.strip_prefix(base_path) {
                let modified_time = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                let size = metadata.len();
                all_keys.push((relative_path.to_path_buf(), modified_time, size));
            }
        } else if metadata.is_dir() {
            // Recurse into subdirectories
            Box::pin(traverse_and_get_keys(&path, base_path, all_keys)).await?;
        }
    }
    Ok(())
}




// S3 GET Object Operation (Download)

/// Load object metadata from `.s3meta.<safe_key>` (xattr or .json)
async fn load_object_meta(parent: &Path, safe_key: &str) -> Result<ObjectMeta, AppError> {
    let marker = parent.join(format!(".s3meta.{}", safe_key));
    get(&marker, "user.s3.meta")
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str(&s).ok())
        .ok_or(AppError::NoSuchKey)
}






// -----------------------------------------------------------------------------
//  GET OBJECT – LARGE-FILE OPTIMIZED
// -----------------------------------------------------------------------------

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("GET {}/{}?{:?}", bucket, key, params);

    // 1. Permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    let full_key_path = state.object_path(&bucket, &key);
    let parent = full_key_path.parent().unwrap();
    let safe_key = key.replace('/', "_");

    // 2. Directory (common prefix) → empty 200
    if fs::metadata(&parent).await.map(|m| m.is_dir()).unwrap_or(false)
        && parent.exists()
        && params.get("prefix").map_or(true, |p| key.starts_with(p))
    {
        let mut headers = S3Headers::common_headers();
        headers.insert(header::CONTENT_LENGTH, "0".parse().unwrap());
        return Ok((headers, Body::empty()).into_response());
    }

    // 3. Load bucket versioning flag
    let versioning = get(&bucket_path.join(".s3meta"), "user.s3.meta")
        .ok()
        .flatten()
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v.get("versioning")?.as_bool())
        .unwrap_or(false);

    // 4. Resolve latest version path
    let object_path = if versioning {
        let mut latest_path = None;
        let mut latest_version = 0;

        let mut entries = fs::read_dir(parent).await.context("read dir")?;
        while let Some(entry) = entries.next_entry().await.context("next entry")? {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with(&key) {
                    if let Some(v_str) = name.rsplit('.').nth(1).and_then(|s| s.strip_prefix('v')) {
                        if let Ok(v) = v_str.parse::<u64>() {
                            if v > latest_version {
                                latest_version = v;
                                latest_path = Some(path);
                            }
                        }
                    }
                }
            }
        }
        latest_path.unwrap_or_else(|| parent.join(&key))
    } else {
        parent.join(&key)
    };

    // 5. Open file + get metadata
    let mut file = File::open(&object_path).await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                AppError::NoSuchKey
            } else {
                AppError::Io(e)
            }
        })
        .context("open file")?;

    let file_meta = file.metadata().await.context("file metadata")?;
    let total_size = file_meta.len();

    // 6. Load S3 metadata
    let obj_meta = load_object_meta(parent, &safe_key).await?;

    // 7. Range request handling
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let (start, end, is_partial) = if let Some(range) = range_header {
        let ranges = HttpRange::parse(range, total_size)
            .map_err(|_| AppError::InvalidArgument("Invalid Range header".into()))?;

        if ranges.is_empty() || ranges.len() > 1 {
            return Err(AppError::InvalidArgument("Multiple ranges not supported".into()));
        }

        let range = &ranges[0];
        let start = range.start;
        let end = start + range.length - 1;

        (start, end, true)
    } else {
        (0, total_size.saturating_sub(1), false)
    };

    let content_length = end - start + 1;

    // === PLATFORM SAFETY: Reject if file or range exceeds addressable size ===
    if total_size > usize::MAX as u64 {
        return Err(AppError::Internal(anyhow::anyhow!(
            "File size {} exceeds platform limit (max: {} bytes)",
            total_size,
            usize::MAX
        )));
    }

    if content_length > usize::MAX as u64 {
        return Err(AppError::Internal(anyhow::anyhow!(
            "Requested range {} bytes exceeds platform limit (max: {} bytes)",
            content_length,
            usize::MAX
        )));
    }

    // Seek to start (only if range request)
    if is_partial {
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .context("seek file")?;
    }

    // === SAFE CHUNKING: Works on 32-bit and 64-bit ===
        // === SAFE CHUNKING: Works on 32-bit and 64-bit ===
    let chunk_size = 1_048_576_u64; // 1 MiB
    let chunk_size_usize: usize = chunk_size
        .try_into()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Chunk size overflow")))?;

    let chunks_needed = if content_length == 0 {
        0
    } else {
        let n = (content_length + chunk_size - 1) / chunk_size;
        n.try_into()
            .map_err(|_| AppError::Internal(anyhow::anyhow!(
                "Number of chunks {} exceeds usize limit", n
            )))?
    };

    let stream = ReaderStream::with_capacity(file, chunk_size_usize)
        .take(chunks_needed);

    let body = Body::from_stream(stream);

    // 9. Build headers
    let mut headers = S3Headers::common_headers();
    headers.insert(header::CONTENT_TYPE, obj_meta.content_type.parse().unwrap());
    headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    headers.insert(header::ETAG, obj_meta.etag.parse().unwrap());
    headers.insert(
        header::LAST_MODIFIED,
        HeaderValue::from_str(&httpdate::fmt_http_date(
            file_meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        ))
        .expect("valid HTTP date"),
    );

    if versioning {
        if let Some(v) = obj_meta.version_id {
            headers.insert("x-amz-version-id", v.to_string().parse().unwrap());
        }
    }

    if is_partial {
        headers.insert(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
        );
        headers.insert(header::CONTENT_LENGTH, content_length.to_string().parse().unwrap());
        tracing::info!(
            "Serving range {}-{} ({}) of {} ({} bytes total)",
            start,
            end,
            content_length,
            object_path.display(),
            total_size
        );
    } else {
        headers.insert(header::CONTENT_LENGTH, total_size.to_string().parse().unwrap());
        tracing::info!(
            "Streaming full {} ({} bytes) | ETag: {} | v{:?}",
            object_path.display(),
            total_size,
            obj_meta.etag,
            obj_meta.version_id
        );
    }

    // 10. Return response
    let status = if is_partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    Ok((status, headers, body).into_response())
}



// pub async fn get_object(
//     State(state): State<Arc<AppState>>,
//     AxumPath((bucket, key)): AxumPath<(String, String)>,
//     user: axum::Extension<AuthenticatedUser>,
// ) -> Result<Response, AppError> {
//     tracing::info!("GET Request: Bucket='{}', Key='{}', User='{}'", bucket, key, user.0.username);

//     // Check read permission
//     if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
//         .await
//         .map_err(|e| AppError::Internal(e))?
//     {
//         return Err(AppError::AccessDenied);
//     }

//     let path = state.object_path(&bucket, &key);
//     let metadata_path = handler_utils::metadata_path(&path);

//     // Check bucket existence
//     let bucket_path = state.bucket_path(&bucket);
//     if !bucket_path.exists() {
//         return Err(AppError::NotFound(bucket));
//     }

//     // Load file
//     let file = File::open(&path).await.map_err(|e| {
//         if e.kind() == ErrorKind::NotFound {
//             AppError::NoSuchKey
//         } else {
//             AppError::Io(e)
//         }
//     }).context("Failed to open file for GET")?;

//     let metadata = file.metadata().await.context("Failed to get file metadata")?;
//     let content_length = metadata.len();
//     let object_meta = handler_utils::load_metadata(&metadata_path).await?;

//     // Build response
//     let stream = ReaderStream::new(file);
//     let body = Body::from_stream(stream);
//     let headers = S3Headers::object_headers(&object_meta.etag, &object_meta.content_type, content_length);

//     tracing::info!("Streaming object: {} ({} bytes), ETag: {}, User: {}", path.display(), content_length, object_meta.etag, user.0.username);

//     Ok(Response::builder()
//         .status(StatusCode::OK)
//         .header("x-amz-request-id", headers.get("x-amz-request-id").unwrap())
//         .header("x-amz-id-2", headers.get("x-amz-id-2").unwrap())
//         .header("server", "AmazonS3")
//         .header("date", headers.get("date").unwrap())
//         .header(header::CONTENT_TYPE, &object_meta.content_type)
//         .header(header::CONTENT_LENGTH, content_length.to_string())
//         .header(header::ETAG, &object_meta.etag)
//         .header(
//             header::LAST_MODIFIED,
//             metadata.modified()
//                 .unwrap_or(SystemTime::UNIX_EPOCH)
//                 .duration_since(SystemTime::UNIX_EPOCH)
//                 .map(|d| chrono::Utc::now() - chrono::Duration::seconds(d.as_secs() as i64))
//                 .unwrap_or(chrono::Utc::now())
//                 .format("%a, %d %b %Y %H:%M:%S GMT")
//                 .to_string()
//         )
//         .body(body)
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to build response: {}", e)))?
//         .into_response())
// }

// S3 DELETE Object Operation
pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    tracing::info!("DELETE Request: Bucket='{}', Key='{}', User='{}'", bucket, key, user.0.username);

    // Check write permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.object_path(&bucket, &key);
    let metadata_path = handler_utils::metadata_path(&path);

    // Check bucket existence
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    let main_delete_result = fs::remove_file(&path).await;
    let meta_delete_result = fs::remove_file(&metadata_path).await;

    match (main_delete_result, meta_delete_result) {
        (Ok(_), _) | (_, Ok(_)) => {
            tracing::info!("Object deleted: {}, User: {}", path.display(), user.0.username);
            let headers = S3Headers::common_headers();
            Ok((StatusCode::NO_CONTENT, headers))
        }
        (Err(e1), Err(e2)) if e1.kind() == ErrorKind::NotFound && e2.kind() == ErrorKind::NotFound => {
            tracing::info!("Object not found, returning 204: {}, User: {}", path.display(), user.0.username);
            let headers = S3Headers::common_headers();
            Ok((StatusCode::NO_CONTENT, headers))
        }
        (Err(e), _) if e.kind() != ErrorKind::NotFound => {
            tracing::error!("Failed to delete object {}: {}, User: {}", path.display(), e, user.0.username);
            Err(AppError::Io(e))
        }
        (_, Err(e)) if e.kind() != ErrorKind::NotFound => {
            tracing::error!("Failed to delete metadata {}: {}, User: {}", metadata_path.display(), e, user.0.username);
            Err(AppError::Io(e))
        }
        _ => {
            tracing::error!("Unforeseen deletion error for {}, User: {}", path.display(), user.0.username);
            Err(AppError::Internal(anyhow::anyhow!("Unforeseen deletion error")))
        }
    }
}

// S3 HEAD Object Operation
pub async fn head_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<Response, AppError> {
    tracing::info!("HEAD Request: Bucket='{}', Key='{}', User='{}'", bucket, key, user.0.username);

    // Check read permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.object_path(&bucket, &key);
    let metadata_path = handler_utils::metadata_path(&path);

    // Check bucket existence
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    // Get filesystem metadata
    let metadata = fs::metadata(&path).await.map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            AppError::NoSuchKey
        } else {
            AppError::Io(e)
        }
    }).context("Failed to get file metadata for HEAD")?;

    let content_length = metadata.len();
    let modified_time: SystemTime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let datetime: DateTime<Utc> = modified_time.into();
    let last_modified_str = datetime.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    let object_meta = handler_utils::load_metadata(&metadata_path).await?;

    let headers = S3Headers::object_headers(&object_meta.etag, &object_meta.content_type, content_length);

    tracing::info!("HEAD success: {} ({} bytes), ETag: {}, User: {}", path.display(), content_length, object_meta.etag, user.0.username);

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to build HEAD response: {}", e)))?
        .into_response();

    response.headers_mut().insert("x-amz-request-id", headers.get("x-amz-request-id").unwrap().clone());
    response.headers_mut().insert("x-amz-id-2", headers.get("x-amz-id-2").unwrap().clone());
    response.headers_mut().insert("server", "AmazonS3".parse().unwrap());
    response.headers_mut().insert(header::LAST_MODIFIED, last_modified_str.parse().unwrap());

    Ok(response)
}

// S3 LIST Objects Operation
pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListObjectsQuery>,
    AxumPath(bucket): AxumPath<String>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    tracing::info!("LIST Request: Bucket='{}', Query={:?}, User='{}'", bucket, query, user.0.username);

    // Check read permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let bucket_root_path = state.bucket_path(&bucket);
    let mut objects: Vec<ObjectInfo> = Vec::new();
    let mut common_prefixes_strings: HashSet<String> = HashSet::new();
    let mut all_keys_metadata = Vec::new();

    if !bucket_root_path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    // Get all keys
    traverse_and_get_keys(&bucket_root_path, &bucket_root_path, &mut all_keys_metadata).await?;
    all_keys_metadata.sort_by(|a, b| a.0.cmp(&b.0));

    let prefix = query.prefix.unwrap_or_default();
    let delimiter = query.delimiter.unwrap_or_default();
    let max_keys = query.max_keys.unwrap_or(1000);
    let marker = query.marker.unwrap_or_default();
    let mut keys_processed = 0;

    for (key_path, modified_time, size_bytes) in all_keys_metadata {
        let key = key_path.to_string_lossy().replace("\\", "/");

        if !marker.is_empty() && key <= marker {
            continue;
        }

        if !prefix.is_empty() && !key.starts_with(&prefix) {
            continue;
        }

        if keys_processed >= max_keys {
            break;
        }

        if !delimiter.is_empty() {
            if let Some(pos) = key[prefix.len()..].find(&delimiter) {
                let common_prefix = &key[0..prefix.len() + pos + delimiter.len()];
                common_prefixes_strings.insert(common_prefix.to_string());
                continue;
            }
        }

        let datetime: DateTime<Utc> = modified_time.into();
        let last_modified_str = datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        let absolute_path = bucket_root_path.join(&key_path);
        let absolute_metadata_path = handler_utils::metadata_path(&absolute_path);
        let object_meta = handler_utils::load_metadata(&absolute_metadata_path).await?;

        objects.push(ObjectInfo {
            key: key.to_string(),
            last_modified: last_modified_str,
            size_bytes,
            etag: object_meta.etag,
        });

        keys_processed += 1;
    }

    let response_data = ListBucketResult {
        xmlns: S3_XMLNS,
        bucket_name: bucket.clone(),
        prefix: if prefix.is_empty() { None } else { Some(prefix) },
        delimiter: if delimiter.is_empty() { None } else { Some(delimiter) },
        marker: if marker.is_empty() { None } else { Some(marker) },
        max_keys,
        is_truncated: keys_processed >= max_keys,
        objects,
        common_prefixes: common_prefixes_strings.into_iter().map(|p| CommonPrefix { prefix: p }).collect(),
    };

    let xml_body = to_xml_string(&response_data).context("Failed to serialize object list to XML")?;

    let headers = S3Headers::xml_headers();
    Ok((headers, xml_body))
}