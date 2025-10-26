use axum::{
    extract::{Path as AxumPath, State, Query},
    response::{Response, IntoResponse},
    body::Body,
    http::{StatusCode, header, HeaderMap},
    extract::Request,
};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::{fs::{File, self}, io::{AsyncWriteExt}};
use tokio_util::io::ReaderStream;
use futures::stream::StreamExt;
use std::{io::ErrorKind, time::SystemTime, collections::HashSet, sync::Arc};
use anyhow::Context;
use chrono::{DateTime, Utc};
use md5;
use hex;
use crate::{
    s3_operations::{handler_utils, auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel}},
    AppError, AppState, ListBucketResult, ObjectInfo, CommonPrefix,
    S3_XMLNS, to_xml_string, S3Headers,
};

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

// S3 PUT Object Operation (Upload)
pub async fn put_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: axum::Extension<AuthenticatedUser>,
    req: Request,
) -> Result<(StatusCode, HeaderMap), AppError> {
    tracing::info!("PUT Request: Bucket='{}', Key='{}', User='{}'", bucket, key, user.0.username);

    // Check write permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.object_path(&bucket, &key);
    let metadata_path = handler_utils::metadata_path(&path);

    // Check if bucket exists
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    let content_type = req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    // Ensure bucket directory exists
    let bucket_dir = path.parent().ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid object path parent")))?;
    fs::create_dir_all(bucket_dir).await.context("Failed to create bucket directory")?;

    // Stream the body to file and calculate MD5
    let mut md5_context = md5::Context::new();
    let mut file = File::create(&path).await.context("Failed to create file for PUT")?;
    let body = req.into_body();
    let mut stream = body.into_data_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Failed to get next chunk from request body")?;
        md5_context.consume(&chunk);
        file.write_all(&chunk).await.context("Failed to write chunk to file")?;
    }

    file.flush().await.context("Failed to flush file data")?;

    // Calculate ETag
    let digest_array: [u8; 16] = md5_context.finalize().into();
    let etag = format!("\"{}\"", hex::encode(digest_array));

    // Save metadata
    let metadata = handler_utils::ObjectMetadata {
        etag: etag.clone(),
        content_type: content_type.clone(),
    };

    let metadata_json = serde_json::to_string_pretty(&metadata)
        .context("Failed to serialize metadata to JSON")?;
    fs::write(&metadata_path, metadata_json)
        .await
        .context("Failed to write metadata file")?;

    tracing::info!("Object saved: {} | ETag: {} | User: {}", path.display(), etag, user.0.username);

    let headers = S3Headers::common_headers();
    Ok((StatusCode::OK, headers))
}

// S3 GET Object Operation (Download)
pub async fn get_object(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<Response, AppError> {
    tracing::info!("GET Request: Bucket='{}', Key='{}', User='{}'", bucket, key, user.0.username);

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

    // Load file
    let file = File::open(&path).await.map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            AppError::NoSuchKey
        } else {
            AppError::Io(e)
        }
    }).context("Failed to open file for GET")?;

    let metadata = file.metadata().await.context("Failed to get file metadata")?;
    let content_length = metadata.len();
    let object_meta = handler_utils::load_metadata(&metadata_path).await?;

    // Build response
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    let headers = S3Headers::object_headers(&object_meta.etag, &object_meta.content_type, content_length);

    tracing::info!("Streaming object: {} ({} bytes), ETag: {}, User: {}", path.display(), content_length, object_meta.etag, user.0.username);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("x-amz-request-id", headers.get("x-amz-request-id").unwrap())
        .header("x-amz-id-2", headers.get("x-amz-id-2").unwrap())
        .header("server", "AmazonS3")
        .header("date", headers.get("date").unwrap())
        .header(header::CONTENT_TYPE, &object_meta.content_type)
        .header(header::CONTENT_LENGTH, content_length.to_string())
        .header(header::ETAG, &object_meta.etag)
        .header(
            header::LAST_MODIFIED,
            metadata.modified()
                .unwrap_or(SystemTime::UNIX_EPOCH)
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| chrono::Utc::now() - chrono::Duration::seconds(d.as_secs() as i64))
                .unwrap_or(chrono::Utc::now())
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
        )
        .body(body)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to build response: {}", e)))?
        .into_response())
}

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