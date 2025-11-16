// use axum::{
//     extract::{Path as AxumPath, State, Query},
//     response::{Response, IntoResponse},
//     body::Body,
//     http::{StatusCode, header, HeaderMap,HeaderValue},
//     extract::Request,Extension
// };
// use std::path::{Path, PathBuf};
// use tokio::{fs::{File, self}, io::{AsyncWriteExt}};
// use tokio_util::io::ReaderStream;
// use futures_util::StreamExt;
// use std::{io::ErrorKind, time::SystemTime, collections::HashSet, sync::Arc};
// use std::collections::VecDeque;
// use anyhow::Context;
// use chrono::{DateTime, Utc};
// use md5;
// use hex;
// use tracing::info;
// use std::convert::TryInto;
// use crate::{
//     s3_operations::{handler_utils, auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel},metadata::{get, set},bucket_handlers::BucketMeta},
//     AppError, AppState, ListBucketResult, ObjectInfo, CommonPrefix,
//     S3_XMLNS, to_xml_string, S3Headers,
// };
// use serde::{Serialize, Deserialize,};
// use http_range::HttpRange;
// use httpdate::fmt_http_date;
// use tokio::io::AsyncSeekExt;
// use std::sync::atomic::{AtomicU64, Ordering};
// use md5::Context as Md5Context;
// use std::collections::HashMap;
// use core::mem; // or `std::mem`
// #[cfg(unix)]
// use std::os::unix::io::AsRawFd;

// /// S3 List Objects Query Parameters
// #[derive(Deserialize, Debug)]
// pub struct ListObjectsQuery {
//     pub prefix: Option<String>,
//     pub delimiter: Option<String>,
//     #[serde(rename = "max-keys")]
//     pub max_keys: Option<u32>,
//     pub marker: Option<String>,
//     #[serde(rename = "versions")]
//     pub list_versions: Option<bool>, // NEW
// }

// #[derive(Serialize)]
// struct ListVersionsResult {
//     #[serde(rename = "Name")] name: String,
//     #[serde(rename = "Prefix")] prefix: Option<String>,
//     #[serde(rename = "KeyMarker")] key_marker: Option<String>,
//     #[serde(rename = "MaxKeys")] max_keys: u32,
//     #[serde(rename = "IsTruncated")] is_truncated: bool,
//     #[serde(rename = "Version")] versions: Vec<VersionEntry>,
//     #[serde(rename = "DeleteMarker")] delete_markers: Vec<DeleteMarkerEntry>,
//     #[serde(rename = "CommonPrefixes")] common_prefixes: Vec<CommonPrefix>,
// }

// #[derive(Serialize)]
// struct VersionEntry {
//     #[serde(rename = "Key")] key: String,
//     #[serde(rename = "VersionId")] version_id: String,
//     #[serde(rename = "IsLatest")] is_latest: bool,
//     #[serde(rename = "LastModified")] last_modified: String,
//     #[serde(rename = "ETag")] etag: String,
//     #[serde(rename = "Size")] size: u64,
// }

// #[derive(Serialize)]
// struct DeleteMarkerEntry {
//     #[serde(rename = "Key")] key: String,
//     #[serde(rename = "VersionId")] version_id: String,
//     #[serde(rename = "IsLatest")] is_latest: bool,
//     #[serde(rename = "LastModified")] last_modified: String,
// }



// #[derive(Debug, Serialize, Deserialize, Default)]
// struct ObjectMeta {
//     #[serde(default)]
//     etag: String,
//     #[serde(default)] 
//     content_type: String,
//     #[serde(default)] 
//     owner: String,
//     #[serde(default)] 
//     current_version: u64,
//     #[serde(default, skip_serializing_if = "Option::is_none")] 
//     version_id: Option<u64>,
//      #[serde(default)]
//     is_delete_marker: bool,
// }

// // impl ObjectMeta {
// //     fn new(owner: &str) -> Self {
// //         Self {
// //             owner: owner.to_string(),
// //             ..Default::default()
// //         }
// //     }
// // }

// impl ObjectMeta {
//     pub fn new(owner: &str) -> Self {
//         Self {
//             etag: "".to_string(),
//             content_type: "application/octet-stream".to_string(),
//             owner: owner.to_string(),
//             current_version: 0,
//             version_id: None,
//             is_delete_marker: false,  // ← CRITICAL
//             ..Default::default()
//         }
//     }
// }


// // // S3 PUT Object Operation (Upload)
// // pub async fn put_object(
// //     State(state): State<Arc<AppState>>,
// //     AxumPath((bucket, key)): AxumPath<(String, String)>,
// //     user: axum::Extension<AuthenticatedUser>,
// //     req: Request,
// // ) -> Result<(StatusCode, HeaderMap), AppError> {
// //     tracing::info!("PUT {}/{}", bucket, key);

// //     // 1. Permission
// //     check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
// //         .await
// //         .map_err(AppError::Internal)?
// //         .then_some(())
// //         .ok_or(AppError::AccessDenied)?;

// //     let base_path = state.object_path(&bucket, &key);
// //     let bucket_path = state.bucket_path(&bucket);
// //     if !bucket_path.exists() { return Err(AppError::NotFound(bucket)); }

// //     let parent = base_path.parent().unwrap();
// //     fs::create_dir_all(parent).await.context("create parent")?;

// //     // 2. Load bucket metadata (for versioning + stats)
// //     let bucket_meta_path = bucket_path.join(".s3meta");
// //     let bucket_meta_json = get(&bucket_meta_path, "user.s3.meta")
// //         .ok()
// //         .flatten()
// //         .and_then(|b| String::from_utf8(b).ok())
// //         .unwrap_or_else(|| r#"{"versioning":false,"object_count":0,"used_bytes":0}"#.to_string());

// //     let mut bucket_meta: BucketMeta = serde_json::from_str(&bucket_meta_json)
// //         .unwrap_or_else(|_| BucketMeta {
// //             versioning: false,
// //             object_count: 0,
// //             used_bytes: 0,
// //             ..Default::default()
// //         });

// //     let versioning = bucket_meta.versioning;

// //     // 3. Shared object metadata marker
// //     let safe_key = key.replace('/', "_");
// //     let meta_marker = parent.join(format!(".s3meta.{}", safe_key));

// //     #[cfg(unix)]
// //     if fs::metadata(&meta_marker).await.is_err() {
// //         fs::write(&meta_marker, b"").await.map_err(AppError::Io)?;
// //     }

// //     // 4. Load object metadata
// //     let mut obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
// //         .ok()
// //         .flatten()
// //         .and_then(|b| String::from_utf8(b).ok())
// //         .and_then(|s| serde_json::from_str(&s).ok())
// //         .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

// //     // 5. Determine object path and version
// //     let (object_path, version_id, is_new_version) = if versioning {
// //         let v = obj_meta.current_version + 1;
// //         (parent.join(format!("{}.v{}", key, v)), Some(v), v == 1)
// //     } else {
// //         (base_path.clone(), None, obj_meta.current_version == 0)
// //     };

// //     // 6. Upload + ETag
// //     let content_type = req.headers()
// //         .get(header::CONTENT_TYPE)
// //         .and_then(|h| h.to_str().ok())
// //         .unwrap_or("application/octet-stream")
// //         .to_string();

// //     let mut hasher = md5::Context::new();
// //     let mut file = fs::File::create(&object_path).await.context("create file")?;
// //     let mut stream = req.into_body().into_data_stream();

// //     while let Some(chunk) = stream.next().await {
// //         let chunk = chunk.context("read chunk")?;
// //         hasher.consume(&chunk);
// //         file.write_all(&chunk).await.context("write chunk")?;
// //     }
// //     file.flush().await.context("flush")?;

// //     let size = fs::metadata(&object_path).await?.len();
// //     let etag = format!("\"{}\"", hex::encode(hasher.finalize().0));

// //     // 7. Update object metadata
// //     obj_meta.etag = etag.clone();
// //     obj_meta.content_type = content_type;
// //     if let Some(v) = version_id {
// //         obj_meta.current_version = v;
// //         obj_meta.version_id = Some(v);
// //     }

// //     let obj_json = serde_json::to_string_pretty(&obj_meta).context("serialize obj meta")?;
// //     set(&meta_marker, "user.s3.meta", obj_json.as_bytes())
// //         .map_err(|e| AppError::Internal(anyhow::anyhow!("write obj meta: {e}")))?;

// //     // 8. Update bucket stats — ONLY on new object/version
// //     if is_new_version {
// //         bucket_meta.object_count += 1;
// //     }
// //     bucket_meta.used_bytes += size;

// //     let bucket_json = serde_json::to_string_pretty(&bucket_meta).context("serialize bucket meta")?;
// //     set(&bucket_meta_path, "user.s3.meta", bucket_json.as_bytes())
// //         .map_err(|e| AppError::Internal(anyhow::anyhow!("write bucket meta: {e}")))?;

// //     // 9. Respond
// //     tracing::info!(
// //         "Uploaded {} | ETag: {} | Size: {} | v{:?} | Bucket: {} objs, {} bytes",
// //         object_path.display(),
// //         etag,
// //         size,
// //         version_id,
// //         bucket_meta.object_count,
// //         bucket_meta.used_bytes
// //     );

// //     let mut headers = S3Headers::common_headers();
// //     headers.insert(header::ETAG, etag.parse().unwrap());
// //     if let Some(v) = version_id {
// //         headers.insert("x-amz-version-id", v.to_string().parse().unwrap());
// //     }

// //     Ok((StatusCode::OK, headers))
// // }


// static PUT_BYTES: AtomicU64 = AtomicU64::new(0);
// static PUT_COUNT: AtomicU64 = AtomicU64::new(0);

// pub async fn put_object(
//     State(state): State<Arc<AppState>>,
//     AxumPath((bucket, key)): AxumPath<(String, String)>,
//     user: Extension<AuthenticatedUser>,
//     req: Request,
// ) -> Result<(StatusCode, HeaderMap), AppError> {
//     PUT_COUNT.fetch_add(1, Ordering::Relaxed);
//     let start = std::time::Instant::now();

//     // 1. Permission
//     check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
//         .await
//         .map_err(AppError::Internal)?
//         .then_some(())
//         .ok_or(AppError::AccessDenied)?;

//     let base_path = state.object_path(&bucket, &key);
//     let bucket_path = state.bucket_path(&bucket);
//     if !bucket_path.exists() {
//         return Err(AppError::NotFound(bucket));
//     }

//     let parent = base_path.parent().unwrap().to_path_buf();
//     fs::create_dir_all(&parent).await.context("create parent")?;

//     // 2. Load bucket metadata — YOUR EXACT PATTERN
//     let bucket_meta_path = bucket_path.join(".s3meta");
//     let bucket_meta_json = get(&bucket_meta_path, "user.s3.meta")
//         .await
//         .ok()
//         .flatten()
//         .and_then(|b| String::from_utf8(b).ok())
//         .unwrap_or_else(|| r#"{"versioning":false,"object_count":0,"used_bytes":0}"#.to_string());

//     let mut bucket_meta: BucketMeta = serde_json::from_str(&bucket_meta_json)
//         .unwrap_or_else(|_| BucketMeta {
//             versioning: false,
//             object_count: 0,
//             used_bytes: 0,
//             ..Default::default()
//         });

//     let versioning = bucket_meta.versioning;

//     // 3. Object metadata marker
//     let safe_key = key.replace('/', "_");
//     let meta_marker = parent.join(format!(".s3meta.{}", safe_key));

//     #[cfg(unix)]
//     if fs::metadata(&meta_marker).await.is_err() {
//         fs::write(&meta_marker, b"").await.map_err(AppError::Io)?;
//     }

//     // 4. Load object metadata — YOUR EXACT PATTERN
//     let mut obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
//         .await
//         .ok()
//         .flatten()
//         .and_then(|b| String::from_utf8(b).ok())
//         .and_then(|s| serde_json::from_str(&s).ok())
//         .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

//     // 5. Final path + version
//     let (final_path, version_id, is_new_version) = if versioning {
//         let v = obj_meta.current_version + 1;
//         (parent.join(format!("{}.v{}", key, v)), Some(v), v == 1)
//     } else {
//         (base_path.clone(), None, obj_meta.current_version == 0)
//     };

//     // 6. READ CONTENT-TYPE BEFORE CONSUMING req
//     let content_type = req.headers()
//         .get(header::CONTENT_TYPE)
//         .and_then(|h| h.to_str().ok())
//         .unwrap_or("application/octet-stream")
//         .to_string();

//     // 7. Temp file (atomic)
//     let temp_path = final_path.with_extension("tmp");
//     let mut file = fs::File::create(&temp_path).await.context("create temp")?;
//     let mut hasher = Md5Context::new();
//     let mut total_size = 0u64;

//     // 8. Stream + hash + write — NOW SAFE
//     let mut stream = req.into_body().into_data_stream();
//     while let Some(chunk) = stream.next().await {
//         let chunk = chunk.context("read chunk")?;
//         let bytes = chunk.as_ref();

//         hasher.consume(bytes);
//         file.write_all(bytes).await.context("write")?;
//         total_size += bytes.len() as u64;
//     }

//     file.flush().await.context("flush")?;
//     file.sync_all().await.context("fsync")?;

//     // 9. Atomic rename
//     fs::rename(&temp_path, &final_path).await.context("rename")?;

//     // 10. ETag
//     let digest = hasher.finalize();
//     let etag = format!("\"{}\"", hex::encode(digest.0));

//     // 11. Update object metadata
//     obj_meta.etag = etag.clone();
//     obj_meta.content_type = content_type;  // ← From earlier

//     if let Some(v) = version_id {
//         obj_meta.current_version = v;
//         obj_meta.version_id = Some(v);
//     }

//     let obj_json = serde_json::to_string_pretty(&obj_meta).context("serialize obj")?;
//     set(&meta_marker, "user.s3.meta", obj_json.as_bytes())
//         .await
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("set obj meta: {e}")))?;

//     // 12. Update bucket stats
//     if is_new_version {
//         bucket_meta.object_count += 1;
//     }
//     bucket_meta.used_bytes += total_size;

//     let bucket_json = serde_json::to_string_pretty(&bucket_meta).context("serialize bucket")?;
//     set(&bucket_meta_path, "user.s3.meta", bucket_json.as_bytes())
//         .await
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("set bucket meta: {e}")))?;

//     // 13. Metrics
//     PUT_BYTES.fetch_add(total_size, Ordering::Relaxed);
//     tracing::info!(
//         "PUT {}/{} | {} bytes | {} ms | ETag: {}",
//         bucket, key, total_size, start.elapsed().as_millis(), etag
//     );

//     // 14. Response
//     let mut headers = S3Headers::common_headers();
//     headers.insert(header::ETAG, etag.parse().unwrap());
//     if let Some(v) = version_id {
//         headers.insert("x-amz-version-id", v.to_string().parse().unwrap());
//     }

//     Ok((StatusCode::OK, headers))
// }


// // /// Helper function to recursively traverse a directory and collect all file paths
// // /// relative to the base_path (the bucket root), while IGNORING metadata files.
// // async fn traverse_and_get_keys(
// //     current_path: &Path,
// //     base_path: &Path,
// //     all_keys: &mut Vec<(PathBuf, SystemTime, u64)>, // (Relative Key, Modified Time, Size)
// // ) -> Result<(), AppError> {
// //     let mut entries = fs::read_dir(current_path).await.context("Failed to read directory")?;

// //     while let Some(entry) = entries.next_entry().await.context("Failed to read directory entry")? {
// //         let path = entry.path();
// //         let metadata = match fs::symlink_metadata(&path).await {
// //             Ok(meta) => meta,
// //             Err(_) => continue, // Skip entries with inaccessible metadata
// //         };

// //         // Skip symbolic links to avoid infinite recursion
// //         if metadata.is_symlink() {
// //             tracing::warn!("Skipping symbolic link: {}", path.display());
// //             continue;
// //         }

// //         if metadata.is_file() {
// //             // Skip metadata files
// //             if let Some(file_name) = path.file_name().and_then(|name| name.to_str()) {
// //                 if file_name.ends_with(".metadata.json") {
// //                     continue;
// //                 }
// //             }

// //             // Calculate the key relative to the bucket root
// //             if let Ok(relative_path) = path.strip_prefix(base_path) {
// //                 let modified_time = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
// //                 let size = metadata.len();
// //                 all_keys.push((relative_path.to_path_buf(), modified_time, size));
// //             }
// //         } else if metadata.is_dir() {
// //             // Recurse into subdirectories
// //             Box::pin(traverse_and_get_keys(&path, base_path, all_keys)).await?;
// //         }
// //     }
// //     Ok(())
// // }




// // S3 GET Object Operation (Download)

// /// Load object metadata from `.s3meta.<safe_key>` (xattr or .json)
// pub async fn load_object_meta(parent: &Path, safe_key: &str) -> Result<ObjectMeta, AppError> {
//     let marker = parent.join(format!(".s3meta.{}", safe_key));

//     // Async get → .await
//     let data = get(&marker, "user.s3.meta")
//         .await
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("metadata read error: {e}")))?;

//     // Your exact pattern
//     data.and_then(|b| String::from_utf8(b).ok())
//             .and_then(|s| serde_json::from_str(&s).ok())
//             .ok_or(AppError::NoSuchKey)
// }

// pub async fn save_object_meta(parent: &Path, safe_key: &str, meta: &ObjectMeta) -> Result<(), AppError> {
//     let marker = parent.join(format!(".s3meta.{}", safe_key));
//     let json = serde_json::to_string_pretty(meta)
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("serialize error: {e}")))?;

//     set(&marker, "user.s3.meta", json.as_bytes())
//         .await
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("metadata write error: {e}")))?;

//     Ok(())
// }






// // -----------------------------------------------------------------------------
// //  GET OBJECT – LARGE-FILE OPTIMIZED
// // -----------------------------------------------------------------------------


// // ──────────────────────────────────────────────────────
// // INDUSTRIAL-GRADE GET OBJECT (32/64-bit safe, MinIO-grade)
// // ──────────────────────────────────────────────────────
// #[cfg(unix)]
// use std::os::unix::io::AsRawFd;

// #[cfg(unix)]
// async fn try_sendfile(
//     file: &mut File,
//     offset: u64,
//     len: u64,
//     response: &mut axum::response::Response,
// ) -> Result<bool, AppError> {
//     use libc::{c_int, off_t, sendfile, ssize_t};

//     unsafe extern "C" fn raw_sendfile(
//         out_fd: c_int,
//         in_fd: c_int,
//         offset: *mut off_t,
//         count: usize,
//     ) -> ssize_t {
//         sendfile(out_fd, in_fd, offset, count)
//     }

//     // Get socket FD from Axum/Hyper
//     let socket_fd = response
//         .extensions()
//         .get::<hyper::server::conn::Http>()
//         .and_then(|conn| conn.io().as_raw_fd())
//         .ok_or_else(|| AppError::Internal(anyhow::anyhow!("No socket for sendfile")))?;

//     let file_fd = file.as_raw_fd();
//     let mut cur_off = offset as off_t;
//     let mut remaining = len as usize;

//     while remaining > 0 {
//         let sent = unsafe { raw_sendfile(socket_fd, file_fd, &mut cur_off, remaining) };
//         if sent <= 0 {
//             let err = std::io::Error::last_os_error();
//             if err.kind() == std::io::ErrorKind::Interrupted {
//                 continue;
//             }
//             return Ok(false); // fallback
//         }
//         remaining -= sent as usize;
//     }
//     Ok(true)
// }

// #[cfg(not(unix))]
// async fn try_sendfile(
//     _file: &mut File,
//     _offset: u64,
//     _len: u64,
//     _response: &mut axum::response::Response,
// ) -> Result<bool, AppError> {
//     Ok(false)
// }

// // ──────────────────────────────────────────────────────
// // INDUSTRIAL-GRADE GET OBJECT (with sendfile on Linux)
// // ──────────────────────────────────────────────────────
// pub async fn get_object(
//     State(state): State<Arc<AppState>>,
//     AxumPath((bucket, key)): AxumPath<(String, String)>,
//     Query(params): Query<HashMap<String, String>>,
//     headers: HeaderMap,
//     user: Extension<AuthenticatedUser>,
//     _req: Request<Body>,
// ) -> Result<impl IntoResponse, AppError> {
//     info!("GET {}/{}?{:?}", bucket, key, params);

//     // 1. Permission
//     if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
//         .await
//         .map_err(AppError::Internal)?
//     {
//         return Err(AppError::AccessDenied);
//     }

//     let bucket_path = state.bucket_path(&bucket);
//     if !bucket_path.exists() {
//         return Err(AppError::NotFound(bucket));
//     }

//     let full_key_path = state.object_path(&bucket, &key);
//     let parent = full_key_path.parent().unwrap().to_path_buf();
//     let safe_key = key.replace('/', "_");

//     // 2. Common prefix → empty 200
//     if full_key_path.is_dir() && params.get("prefix").map_or(true, |p| key.starts_with(p)) {
//         let mut headers = S3Headers::common_headers();
//         headers.insert(header::CONTENT_LENGTH, "0".parse().unwrap());
//         return Ok((StatusCode::OK, headers, Body::empty()).into_response());
//     }

//     // 3. Load bucket versioning
//     let versioning = get(&bucket_path.join(".s3meta"), "user.s3.meta")
//         .await
//         .ok()
//         .flatten()
//         .and_then(|b| String::from_utf8(b).ok())
//         .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
//         .and_then(|v| v.get("versioning")?.as_bool())
//         .unwrap_or(false);

//     // 4. Resolve latest version
//     let object_path = if versioning {
//         let mut latest_path = None;
//         let mut latest_version = 0u64;
//         let mut entries = tokio::fs::read_dir(&parent).await.context("read dir")?;

//         while let Some(entry) = entries.next_entry().await.context("next entry")? {
//             let path = entry.path();
//             if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
//                 if name.starts_with(&key) && name.contains(".v") {
//                     if let Some(version_part) = name.rsplit('.').nth(0) {
//                         if let Some(v_str) = version_part.strip_prefix('v') {
//                             if let Ok(v) = v_str.parse::<u64>() {
//                                 if v > latest_version {
//                                     latest_version = v;
//                                     latest_path = Some(path);
//                                 }
//                             }
//                         }
//                     }
//                 }
//             }
//         }
//         latest_path.unwrap_or(full_key_path)
//     } else {
//         full_key_path
//     };

//     // 5. Open file
//     let mut file = File::open(&object_path).await
//         .map_err(|e| if e.kind() == std::io::ErrorKind::NotFound {
//             AppError::NoSuchKey
//         } else {
//             AppError::Io(e)
//         })?;

//     let file_meta = file.metadata().await.context("file metadata")?;
//     let total_size = file_meta.len();

//     // 6. Load S3 metadata
//     let obj_meta = load_object_meta(&parent, &safe_key).await?;

//     // 7. Range request
//     let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
//     let (start, end, is_partial) = if let Some(range) = range_header {
//         let ranges = HttpRange::parse(range, total_size)
//             .map_err(|_| AppError::InvalidArgument("Invalid Range".into()))?;
//         if ranges.len() != 1 {
//             return Err(AppError::InvalidArgument("Multiple ranges not supported".into()));
//         }
//         let r = &ranges[0];
//         (r.start, r.start + r.length - 1, true)
//     } else {
//         (0, total_size.saturating_sub(1), false)
//     };

//     let content_length = end - start + 1;

//     // 8. Platform safety: reject if > usize::MAX
//     if total_size > usize::MAX as u64 {
//         return Err(AppError::Internal(anyhow::anyhow!(
//             "File too large for platform (max {} bytes)",
//             usize::MAX
//         )));
//     }

//     // ──────────────────────────────────────────────────────
//     // SEND FILE ACCELERATION (Linux only, full file)
//     // ──────────────────────────────────────────────────────
//     let use_sendfile = cfg!(unix) && !is_partial && content_length == total_size;

//     if use_sendfile {
//         let mut resp = axum::response::Response::builder()
//             .status(StatusCode::OK)
//             .header(header::CONTENT_TYPE, &obj_meta.content_type)
//             .header(header::ACCEPT_RANGES, "bytes")
//             .header(header::ETAG, &obj_meta.etag)
//             .header(
//                 header::LAST_MODIFIED,
//                 HeaderValue::from_str(&httpdate::fmt_http_date(file_meta.modified()?))
//                     .expect("valid HTTP date"),
//             )
//             .header(header::CONTENT_LENGTH, total_size.to_string());

//         if versioning {
//             if let Some(v) = obj_meta.version_id {
//                 resp = resp.header("x-amz-version-id", v.to_string());
//             }
//         }

//         let mut response = resp.body(Body::empty())
//             .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

//         if try_sendfile(&mut file, 0, total_size, &mut response).await? {
//             info!(
//                 "sendfile() served {object_path:?} | {total_size} bytes | ETag: {}",
//                 obj_meta.etag
//             );
//             return Ok(response);
//         }
//         // Fallback to streaming if sendfile fails
//     }

//     // ──────────────────────────────────────────────────────
//     // FALLBACK: Zero-copy ReaderStream (your original path)
//     // ──────────────────────────────────────────────────────
//     const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
//     const MAX_32BIT_SAFE: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB
//     let chunk_size_u64 = CHUNK_SIZE as u64;

//     let num_chunks_u64 = if content_length == 0 {
//         0
//     } else {
//         (content_length + chunk_size_u64 - 1) / chunk_size_u64
//     };

//     // SEEK BEFORE STREAM (critical!)
//     if is_partial {
//         file.seek(std::io::SeekFrom::Start(start)).await.context("seek")?;
//     }

//     let (body, response_content_length): (Body, u64) = if mem::size_of::<usize>() == 8 {
//         // 64-bit: always chunk
//         let stream = ReaderStream::with_capacity(file, CHUNK_SIZE)
//             .take(num_chunks_u64 as usize);
//         (Body::from_stream(stream), content_length)
//     } else {
//         // 32-bit
//         if total_size > MAX_32BIT_SAFE {
//             let stream = ReaderStream::with_capacity(file, CHUNK_SIZE);
//             (Body::from_stream(stream), total_size)
//         } else {
//             let limit: usize = num_chunks_u64.try_into().map_err(|_| {
//                 AppError::Internal(anyhow::anyhow!("Chunk count exceeds usize::MAX on 32-bit"))
//             })?;
//             let stream = ReaderStream::with_capacity(file, CHUNK_SIZE)
//                 .take(limit);
//             (Body::from_stream(stream), content_length)
//         }
//     };

//     // 10. Headers
//     let mut headers = S3Headers::common_headers();
//     headers.insert(header::CONTENT_TYPE, obj_meta.content_type.parse().unwrap());
//     headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
//     headers.insert(header::ETAG, obj_meta.etag.parse().unwrap());
//     headers.insert(
//         header::LAST_MODIFIED,
//         HeaderValue::from_str(&httpdate::fmt_http_date(file_meta.modified()?))
//             .expect("valid HTTP date"),
//     );

//     if versioning {
//         if let Some(v) = obj_meta.version_id {
//             headers.insert("x-amz-version-id", v.to_string().parse().unwrap());
//         }
//     }

//     if is_partial {
//         headers.insert(
//             header::CONTENT_RANGE,
//             format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
//         );
//     }
//     headers.insert(
//         header::CONTENT_LENGTH,
//         response_content_length.to_string().parse().unwrap(),
//     );

//     let status = if is_partial { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };

//     info!(
//         "Serving {object_path:?} | {response_content_length} bytes | Range: {is_partial} | ETag: {}",
//         obj_meta.etag
//     );

//     Ok((status, headers, body).into_response())
// }

// // pub async fn get_object(
// //     State(state): State<Arc<AppState>>,
// //     AxumPath((bucket, key)): AxumPath<(String, String)>,
// //     Query(params): Query<std::collections::HashMap<String, String>>,
// //     headers: HeaderMap,
// //     user: axum::Extension<AuthenticatedUser>,
// // ) -> Result<impl IntoResponse, AppError> {
// //     tracing::info!("GET {}/{}?{:?}", bucket, key, params);

// //     // 1. Permission
// //     if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
// //         .await
// //         .map_err(AppError::Internal)?
// //     {
// //         return Err(AppError::AccessDenied);
// //     }

// //     let bucket_path = state.bucket_path(&bucket);
// //     if !bucket_path.exists() {
// //         return Err(AppError::NotFound(bucket));
// //     }

// //     let full_key_path = state.object_path(&bucket, &key);
// //     let parent = full_key_path.parent().unwrap();
// //     let safe_key = key.replace('/', "_");

// //     // 2. Directory (common prefix) → empty 200
// //     if fs::metadata(&parent).await.map(|m| m.is_dir()).unwrap_or(false)
// //         && parent.exists()
// //         && params.get("prefix").map_or(true, |p| key.starts_with(p))
// //     {
// //         let mut headers = S3Headers::common_headers();
// //         headers.insert(header::CONTENT_LENGTH, "0".parse().unwrap());
// //         return Ok((headers, Body::empty()).into_response());
// //     }

// //     // 3. Load bucket versioning flag
// //     let versioning = get(&bucket_path.join(".s3meta"), "user.s3.meta")
// //         .ok()
// //         .flatten()
// //         .and_then(|b| String::from_utf8(b).ok())
// //         .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
// //         .and_then(|v| v.get("versioning")?.as_bool())
// //         .unwrap_or(false);

// //     // 4. Resolve latest version path
// //     let object_path = if versioning {
// //         let mut latest_path = None;
// //         let mut latest_version = 0;

// //         let mut entries = fs::read_dir(parent).await.context("read dir")?;
// //         while let Some(entry) = entries.next_entry().await.context("next entry")? {
// //             let path = entry.path();
// //             if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
// //                 if name.starts_with(&key) {
// //                     if let Some(v_str) = name.rsplit('.').nth(1).and_then(|s| s.strip_prefix('v')) {
// //                         if let Ok(v) = v_str.parse::<u64>() {
// //                             if v > latest_version {
// //                                 latest_version = v;
// //                                 latest_path = Some(path);
// //                             }
// //                         }
// //                     }
// //                 }
// //             }
// //         }
// //         latest_path.unwrap_or_else(|| parent.join(&key))
// //     } else {
// //         parent.join(&key)
// //     };

// //     // 5. Open file + get metadata
// //     let mut file = File::open(&object_path).await
// //         .map_err(|e| {
// //             if e.kind() == std::io::ErrorKind::NotFound {
// //                 AppError::NoSuchKey
// //             } else {
// //                 AppError::Io(e)
// //             }
// //         })
// //         .context("open file")?;

// //     let file_meta = file.metadata().await.context("file metadata")?;
// //     let total_size = file_meta.len();

// //     // 6. Load S3 metadata
// //     let obj_meta = load_object_meta(parent, &safe_key).await?;

// //     // 7. Range request handling
// //     let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
// //     let (start, end, is_partial) = if let Some(range) = range_header {
// //         let ranges = HttpRange::parse(range, total_size)
// //             .map_err(|_| AppError::InvalidArgument("Invalid Range header".into()))?;

// //         if ranges.is_empty() || ranges.len() > 1 {
// //             return Err(AppError::InvalidArgument("Multiple ranges not supported".into()));
// //         }

// //         let range = &ranges[0];
// //         let start = range.start;
// //         let end = start + range.length - 1;

// //         (start, end, true)
// //     } else {
// //         (0, total_size.saturating_sub(1), false)
// //     };

// //     let content_length = end - start + 1;

// //     // === PLATFORM SAFETY: Reject if file or range exceeds addressable size ===
// //     if total_size > usize::MAX as u64 {
// //         return Err(AppError::Internal(anyhow::anyhow!(
// //             "File size {} exceeds platform limit (max: {} bytes)",
// //             total_size,
// //             usize::MAX
// //         )));
// //     }

// //     if content_length > usize::MAX as u64 {
// //         return Err(AppError::Internal(anyhow::anyhow!(
// //             "Requested range {} bytes exceeds platform limit (max: {} bytes)",
// //             content_length,
// //             usize::MAX
// //         )));
// //     }

// //     // Seek to start (only if range request)
// //     if is_partial {
// //         file.seek(std::io::SeekFrom::Start(start))
// //             .await
// //             .context("seek file")?;
// //     }

// //     // === SAFE CHUNKING: Works on 32-bit and 64-bit ===
// //     let chunk_size = 1_048_576_u64; // 1 MiB
// //     let chunk_size_usize: usize = chunk_size
// //         .try_into()
// //         .map_err(|_| AppError::Internal(anyhow::anyhow!("Chunk size overflow")))?;

// //     let chunks_needed = if content_length == 0 {
// //         0
// //     } else {
// //         let n = (content_length + chunk_size - 1) / chunk_size;
// //         n.try_into()
// //             .map_err(|_| AppError::Internal(anyhow::anyhow!(
// //                 "Number of chunks {} exceeds usize limit", n
// //             )))?
// //     };

// //     let stream = ReaderStream::with_capacity(file, chunk_size_usize)
// //         .take(chunks_needed);

// //     let body = Body::from_stream(stream);

// //     // 9. Build headers
// //     let mut headers = S3Headers::common_headers();
// //     headers.insert(header::CONTENT_TYPE, obj_meta.content_type.parse().unwrap());
// //     headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
// //     headers.insert(header::ETAG, obj_meta.etag.parse().unwrap());
// //     headers.insert(
// //         header::LAST_MODIFIED,
// //         HeaderValue::from_str(&httpdate::fmt_http_date(
// //             file_meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
// //         ))
// //         .expect("valid HTTP date"),
// //     );

// //     if versioning {
// //         if let Some(v) = obj_meta.version_id {
// //             headers.insert("x-amz-version-id", v.to_string().parse().unwrap());
// //         }
// //     }

// //     if is_partial {
// //         headers.insert(
// //             header::CONTENT_RANGE,
// //             format!("bytes {start}-{end}/{total_size}").parse().unwrap(),
// //         );
// //         headers.insert(header::CONTENT_LENGTH, content_length.to_string().parse().unwrap());
// //         tracing::info!(
// //             "Serving range {}-{} ({}) of {} ({} bytes total)",
// //             start,
// //             end,
// //             content_length,
// //             object_path.display(),
// //             total_size
// //         );
// //     } else {
// //         headers.insert(header::CONTENT_LENGTH, total_size.to_string().parse().unwrap());
// //         tracing::info!(
// //             "Streaming full {} ({} bytes) | ETag: {} | v{:?}",
// //             object_path.display(),
// //             total_size,
// //             obj_meta.etag,
// //             obj_meta.version_id
// //         );
// //     }

// //     // 10. Return response
// //     let status = if is_partial {
// //         StatusCode::PARTIAL_CONTENT
// //     } else {
// //         StatusCode::OK
// //     };

// //     Ok((status, headers, body).into_response())
// // }







// /// Walk the bucket, skip every `.s3meta*` file and return:
// /// * `files` – (relative Path, mtime, size)
// /// * `dirs`  – (relative Path) of every directory
// pub async fn traverse_and_get_keys(
//     root: &Path,
//     base: &Path,
//     files: &mut Vec<(PathBuf, SystemTime, u64)>,
//     dirs: &mut Vec<String>,
// ) -> Result<(), AppError> {
//     let mut stack = VecDeque::from([root.to_path_buf()]);

//     while let Some(dir) = stack.pop_back() {
//         let mut entries = fs::read_dir(&dir).await.context("read dir")?;

//         while let Some(entry) = entries.next_entry().await.context("next entry")? {
//             let path = entry.path();
//             let meta = fs::symlink_metadata(&path).await.context("metadata")?;
//             if meta.is_symlink() { continue; }

//             let name = path.file_name().and_then(|n| n.to_str());
//             if let Some(n) = name {
//                 if n == ".s3meta" || n == ".s3meta.json" || n.starts_with(".s3meta.") {
//                     continue;
//                 }
//             }

//             if meta.is_file() {
//                 if let Ok(rel) = path.strip_prefix(base) {
//                     files.push((
//                         rel.to_path_buf(),
//                         meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
//                         meta.len(),
//                     ));
//                 }
//             } else if meta.is_dir() {
//                 if let Ok(rel) = path.strip_prefix(base) {
//                     dirs.push(format!("{}/", rel.to_string_lossy().replace('\\', "/")));
//                 }
//                 stack.push_back(path);
//             }
//         }
//     }
//     Ok(())
// }

// // -----------------------------------------------------------------------------
// //  S3 LIST Objects Operation
// // -----------------------------------------------------------------------------

// pub async fn list_objects(
//     State(state): State<Arc<AppState>>,
//     AxumPath(bucket): AxumPath<String>,
//     Query(query): Query<ListObjectsQuery>,
//     user: axum::Extension<AuthenticatedUser>,
// ) -> Result<(HeaderMap, String), AppError> {
//     tracing::info!("LIST {bucket} {query:?} user={}", user.0.username);

//     // --- Permission ---
//     if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
//         .await
//         .map_err(AppError::Internal)?
//     {
//         return Err(AppError::AccessDenied);
//     }

//     let root = state.bucket_path(&bucket);
//     if !root.exists() {
//         return Err(AppError::NotFound(bucket));
//     }

//     // --- Query ---
//     let prefix = query.prefix.unwrap_or_default();
//     let delimiter = query.delimiter.unwrap_or_default();
//     let key_marker = query.marker.unwrap_or_default();
//     let max_keys_s3: u32 = query.max_keys.unwrap_or(1000).min(1000);
//     let max_keys: usize = max_keys_s3 as usize;
//     let list_versions = query.list_versions.unwrap_or(false);

//     // --- Scan ---
//     let mut files = Vec::new();
//     let mut dirs = Vec::new();
//     traverse_and_get_keys(&root, &root, &mut files, &mut dirs).await?;
//     files.sort_by_key(|f| f.0.clone());

//     // --- Versioning: List All Versions ---
//     if list_versions {
//         let mut versions = Vec::new();
//         let mut delete_markers = Vec::new();
//         let mut common_prefixes = HashSet::new();
//         let mut count = 0;

//         for (path, mtime, size) in files {
//             let full_key = path.to_string_lossy().replace('\\', "/");
//             if !prefix.is_empty() && !full_key.starts_with(&prefix) { continue; }
//             if !key_marker.is_empty() && full_key <= key_marker { continue; }

//             // Parse: "photos/pic.jpg.v3" → key="photos/pic.jpg", version=3
//             let (key, version_id) = if let Some((k, v)) = full_key.rsplit_once(".v") {
//                 if let Ok(v) = v.parse::<u64>() {
//                     (k.to_string(), Some(v))
//                 } else {
//                     (full_key.clone(), None)
//                 }
//             } else {
//                 (full_key.clone(), None)
//             };

//             // Load metadata
//             let safe = key.replace('/', "_");
//             let meta_path = root.join(&path).parent().unwrap().join(format!(".s3meta.{safe}"));
//             let meta = get(&meta_path, "user.s3.meta")
//                 .ok().flatten()
//                 .and_then(|b| String::from_utf8(b).ok())
//                 .and_then(|s| serde_json::from_str::<ObjectMeta>(&s).ok())
//                 .unwrap_or_default();

//             let last_modified = DateTime::<Utc>::from(mtime)
//                 .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

//             if meta.is_delete_marker {
//                 delete_markers.push(DeleteMarkerEntry {
//                     key: key.clone(),
//                     version_id: meta.version_id.unwrap_or(0).to_string(),
//                     is_latest: meta.current_version == version_id.unwrap_or(0),
//                     last_modified,
//                 });
//             } else {
//                 versions.push(VersionEntry {
//                     key,
//                     version_id: meta.version_id.unwrap_or(0).to_string(),
//                     is_latest: meta.current_version == version_id.unwrap_or(0),
//                     last_modified,
//                     etag: meta.etag,
//                     size,
//                 });
//             }

//             count += 1;
//             if count >= max_keys { break; }
//         }

//         // Add common prefixes from dirs
//         if !delimiter.is_empty() {
//             for dir in &dirs {
//                 let dir_key = dir.clone();
//                 if !dir_key.starts_with(&prefix) { continue; }
//                 if let Some(rest) = dir_key.strip_prefix(&prefix) {
//                     if let Some(pos) = rest.find(&delimiter) {
//                         common_prefixes.insert(format!("{}{}", prefix, &rest[..pos + delimiter.len()]));
//                     } else if rest.ends_with('/') {
//                         common_prefixes.insert(format!("{prefix}{rest}"));
//                     }
//                 }
//             }
//         }

//         let result = ListVersionsResult {
//             name: bucket,
//             prefix: Some(prefix),
//             key_marker: Some(key_marker),
//             max_keys: max_keys_s3,
//             is_truncated: count >= max_keys,
//             versions,
//             delete_markers,
//             common_prefixes: common_prefixes.into_iter().map(|p| CommonPrefix { prefix: p }).collect(),
//         };

//         let xml = quick_xml::se::to_string(&result)
//             .map_err(|e| AppError::Internal(anyhow::anyhow!("XML: {e}")))?;

//         let mut headers = S3Headers::xml_headers();
//         headers.insert(header::CONTENT_LENGTH, xml.len().to_string().parse().unwrap());
//         return Ok((headers, xml));
//     }

//     // --- Normal ListObjects (latest only) ---
//     let mut objects = Vec::new();
//     let mut prefixes = HashSet::new();
//     let mut count = 0;
//     let mut truncated = false;

//     for (path, mtime, size) in files {
//         let key = path.to_string_lossy().replace('\\', "/");
//         if !prefix.is_empty() && !key.starts_with(&prefix) { continue; }
//         if !key_marker.is_empty() && key <= key_marker { continue; }

//         // Skip versioned files (only show base key)
//         if key.contains(".v") { continue; }

//         if !delimiter.is_empty() {
//             if let Some(rest) = key.strip_prefix(&prefix) {
//                 if let Some(pos) = rest.find(&delimiter) {
//                     prefixes.insert(format!("{}{}", prefix, &rest[..pos + delimiter.len()]));
//                     continue;
//                 }
//             }
//         }

//         if count >= max_keys { truncated = true; break; }

//         let safe = key.replace('/', "_");
//         let meta_path = root.join(&path).parent().unwrap().join(format!(".s3meta.{safe}"));
//         let meta = get(&meta_path, "user.s3.meta")
//             .ok().flatten()
//             .and_then(|b| String::from_utf8(b).ok())
//             .and_then(|s| serde_json::from_str::<ObjectMeta>(&s).ok())
//             .unwrap_or_else(|| ObjectMeta {
//                 etag: format!("\"{}\"", hex::encode(md5::compute(format!("{key}{size}").as_bytes()).0)),
//                 content_type: "application/octet-stream".to_string(),
//                 ..Default::default()
//             });

//         objects.push(ObjectInfo {
//             key,
//             last_modified: DateTime::<Utc>::from(mtime)
//                 .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
//             size_bytes: size,
//             etag: meta.etag,
//         });
//         count += 1;
//     }

//     // Add folders
//     if !delimiter.is_empty() {
//         for dir in &dirs {
//             if !dir.starts_with(&prefix) { continue; }
//             if let Some(rest) = dir.strip_prefix(&prefix) {
//                 if let Some(pos) = rest.find(&delimiter) {
//                     prefixes.insert(format!("{}{}", prefix, &rest[..pos + delimiter.len()]));
//                 } else if rest.ends_with('/') {
//                     prefixes.insert(format!("{prefix}{rest}"));
//                 }
//             }
//         }
//     }

//     let result = ListBucketResult {
//         xmlns: S3_XMLNS,
//         bucket_name: bucket,
//         prefix: (!prefix.is_empty()).then_some(prefix),
//         delimiter: (!delimiter.is_empty()).then_some(delimiter),
//         marker: (!key_marker.is_empty()).then_some(key_marker),
//         max_keys: max_keys_s3,
//         is_truncated: truncated,
//         objects,
//         common_prefixes: prefixes.into_iter().map(|p| CommonPrefix { prefix: p }).collect(),
//     };

//     let xml = quick_xml::se::to_string(&result)
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("XML: {e}")))?;

//     let mut headers = S3Headers::xml_headers();
//     headers.insert(header::CONTENT_LENGTH, xml.len().to_string().parse().unwrap());

//     Ok((headers, xml))
// }

// // pub async fn list_objects(
// //     State(state): State<Arc<AppState>>,
// //     AxumPath(bucket): AxumPath<String>,
// //     Query(query): Query<ListObjectsQuery>,
// //     user: axum::Extension<AuthenticatedUser>,
// // ) -> Result<(HeaderMap, String), AppError> {
// //     tracing::info!(
// //         "LIST Request: Bucket='{}', Query={:?}, User='{}'",
// //         bucket,
// //         query,
// //         user.0.username
// //     );

// //     // ---- 1. Permission ----------------------------------------------------
// //     if !check_bucket_permission(
// //         &state.pool,
// //         &user.0,
// //         &bucket,
// //         PermissionLevel::ReadOnly.as_str(),
// //     )
// //     .await
// //     .map_err(AppError::Internal)?
// //     {
// //         return Err(AppError::AccessDenied);
// //     }

// //     let bucket_root = state.bucket_path(&bucket);
// //     if !bucket_root.exists() {
// //         return Err(AppError::NotFound(bucket));
// //     }

// //     // ---- 2. Query parameters (S3 uses u32) -------------------------------
// //     let prefix = query.prefix.unwrap_or_default();
// //     let delimiter = query.delimiter.unwrap_or_default();
// //     let marker = query.marker.unwrap_or_default();

// //     // S3: max-keys is u32, cap at 1000
// //     let max_keys_s3: u32 = query.max_keys.unwrap_or(1000).min(1000);
// //     let max_keys: usize = max_keys_s3 as usize; // ← 100% safe

// //     // ---- 3. Gather every *real* file (skip .s3meta.*) --------------------
// //     let mut all_keys_metadata = Vec::<(PathBuf, SystemTime, u64)>::new();
// //     traverse_and_get_keys(&bucket_root, &bucket_root, &mut all_keys_metadata).await?;
// //     all_keys_metadata.sort_by(|a, b| a.0.cmp(&b.0));

// //     // ---- 4. Filter & apply S3 semantics ---------------------------------
// //     let mut objects = Vec::<ObjectInfo>::new();
// //     let mut common_prefixes = HashSet::<String>::new();
// //     let mut processed = 0usize;
// //     let mut is_truncated = false;

// //     for (rel_path, modified, size) in all_keys_metadata {
// //         let key = rel_path.to_string_lossy().replace('\\', "/");

// //         // Skip marker
// //         if !marker.is_empty() && key <= marker {
// //             continue;
// //         }

// //         // Skip prefix
// //         if !prefix.is_empty() && !key.starts_with(&prefix) {
// //             continue;
// //         }

// //         // Delimiter → CommonPrefixes
// //         if !delimiter.is_empty() {
// //             if let Some(rest) = key.strip_prefix(&prefix) {
// //                 if let Some(pos) = rest.find(&delimiter) {
// //                     let cp = format!("{}{}", prefix, &rest[..pos + delimiter.len()]);
// //                     common_prefixes.insert(cp);
// //                     continue;
// //                 }
// //             }
// //         }

// //         // Max keys
// //         if processed >= max_keys {
// //             is_truncated = true;
// //             break;
// //         }

// //         // Skip bucket metadata files
// //         if let Some(name) = rel_path.file_name().and_then(|n| n.to_str()) {
// //             if name == ".s3meta" || name == ".s3meta.json" {
// //                 continue;
// //             }
// //         }

// //         // Load object metadata
// //         let safe_key = key.replace('/', "_");
// //         let meta_path = bucket_root.join(&rel_path).parent().unwrap().join(format!(".s3meta.{}", safe_key));

// //         let obj_meta = match get(&meta_path, "user.s3.meta")
// //             .ok()
// //             .flatten()
// //             .and_then(|b| String::from_utf8(b).ok())
// //             .and_then(|s| serde_json::from_str::<ObjectMeta>(&s).ok())
// //         {
// //             Some(meta) => meta,
// //             None => ObjectMeta {
// //                 etag: format!("\"{}\"", hex::encode(md5::compute(format!("{}{}", key, size).as_bytes()).0)),
// //                 content_type: "application/octet-stream".to_string(),
// //                 ..Default::default()
// //             },
// //         };

// //         let last_modified = DateTime::<Utc>::from(modified)
// //             .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

// //         objects.push(ObjectInfo {
// //             key,
// //             last_modified,
// //             size_bytes: size,
// //             etag: obj_meta.etag,
// //         });

// //         processed += 1;
// //     }

// //     // ---- 5. Build XML (S3 expects u32) -----------------------------------
// //     let result = ListBucketResult {
// //         xmlns: S3_XMLNS,
// //         bucket_name: bucket.clone(),
// //         prefix: (!prefix.is_empty()).then_some(prefix),
// //         delimiter: (!delimiter.is_empty()).then_some(delimiter),
// //         marker: (!marker.is_empty()).then_some(marker),
// //         max_keys: max_keys_s3,  // ← u32
// //         is_truncated,
// //         objects,
// //         common_prefixes: common_prefixes
// //             .into_iter()
// //             .map(|p| CommonPrefix { prefix: p })
// //             .collect(),
// //     };

// //     let xml_body = quick_xml::se::to_string(&result)
// //         .map_err(|e| AppError::Internal(anyhow::anyhow!("XML serialization: {e}")))?;

// //     let mut headers = S3Headers::xml_headers();
// //     headers.insert(
// //         header::CONTENT_LENGTH,
// //         HeaderValue::from_str(&xml_body.len().to_string())
// //             .expect("content-length is valid"),
// //     );

// //     Ok((headers, xml_body))
// // }







// // S3 DELETE Object Operation
// pub async fn delete_object(
//     State(state): State<Arc<AppState>>,
//     AxumPath((bucket, key)): AxumPath<(String, String)>,
//     user: axum::Extension<AuthenticatedUser>,
// ) -> Result<(StatusCode, HeaderMap), AppError> {
//     tracing::info!("DELETE Request: Bucket='{}', Key='{}', User='{}'", bucket, key, user.0.username);

//     // Check write permission
//     if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
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

//     let main_delete_result = fs::remove_file(&path).await;
//     let meta_delete_result = fs::remove_file(&metadata_path).await;

//     match (main_delete_result, meta_delete_result) {
//         (Ok(_), _) | (_, Ok(_)) => {
//             tracing::info!("Object deleted: {}, User: {}", path.display(), user.0.username);
//             let headers = S3Headers::common_headers();
//             Ok((StatusCode::NO_CONTENT, headers))
//         }
//         (Err(e1), Err(e2)) if e1.kind() == ErrorKind::NotFound && e2.kind() == ErrorKind::NotFound => {
//             tracing::info!("Object not found, returning 204: {}, User: {}", path.display(), user.0.username);
//             let headers = S3Headers::common_headers();
//             Ok((StatusCode::NO_CONTENT, headers))
//         }
//         (Err(e), _) if e.kind() != ErrorKind::NotFound => {
//             tracing::error!("Failed to delete object {}: {}, User: {}", path.display(), e, user.0.username);
//             Err(AppError::Io(e))
//         }
//         (_, Err(e)) if e.kind() != ErrorKind::NotFound => {
//             tracing::error!("Failed to delete metadata {}: {}, User: {}", metadata_path.display(), e, user.0.username);
//             Err(AppError::Io(e))
//         }
//         _ => {
//             tracing::error!("Unforeseen deletion error for {}, User: {}", path.display(), user.0.username);
//             Err(AppError::Internal(anyhow::anyhow!("Unforeseen deletion error")))
//         }
//     }
// }

// // S3 HEAD Object Operation
// pub async fn head_object(
//     State(state): State<Arc<AppState>>,
//     AxumPath((bucket, key)): AxumPath<(String, String)>,
//     user: axum::Extension<AuthenticatedUser>,
// ) -> Result<Response, AppError> {
//     tracing::info!("HEAD Request: Bucket='{}', Key='{}', User='{}'", bucket, key, user.0.username);

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

//     // Get filesystem metadata
//     let metadata = fs::metadata(&path).await.map_err(|e| {
//         if e.kind() == ErrorKind::NotFound {
//             AppError::NoSuchKey
//         } else {
//             AppError::Io(e)
//         }
//     }).context("Failed to get file metadata for HEAD")?;

//     let content_length = metadata.len();
//     let modified_time: SystemTime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
//     let datetime: DateTime<Utc> = modified_time.into();
//     let last_modified_str = datetime.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

//     let object_meta = handler_utils::load_metadata(&metadata_path).await?;

//     let headers = S3Headers::object_headers(&object_meta.etag, &object_meta.content_type, content_length);

//     tracing::info!("HEAD success: {} ({} bytes), ETag: {}, User: {}", path.display(), content_length, object_meta.etag, user.0.username);

//     let mut response = Response::builder()
//         .status(StatusCode::OK)
//         .body(Body::empty())
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to build HEAD response: {}", e)))?
//         .into_response();

//     response.headers_mut().insert("x-amz-request-id", headers.get("x-amz-request-id").unwrap().clone());
//     response.headers_mut().insert("x-amz-id-2", headers.get("x-amz-id-2").unwrap().clone());
//     response.headers_mut().insert("server", "AmazonS3".parse().unwrap());
//     response.headers_mut().insert(header::LAST_MODIFIED, last_modified_str.parse().unwrap());

//     Ok(response)
// }



// //DELETE WITH MARKER
// // pub async fn delete_object(
// //     State(state): State<Arc<AppState>>,
// //     AxumPath((bucket, key)): AxumPath<(String, String)>,
// //     user: axum::Extension<AuthenticatedUser>,
// // ) -> Result<(StatusCode, HeaderMap), AppError> {
// //     tracing::info!("DELETE {}/{}", bucket, key);

// //     // 1. Permission
// //     check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
// //         .await
// //         .map_err(AppError::Internal)?
// //         .then_some(())
// //         .ok_or(AppError::AccessDenied)?;

// //     let bucket_path = state.bucket_path(&bucket);
// //     if !bucket_path.exists() {
// //         return Err(AppError::NotFound(bucket));
// //     }

// //     let parent = state.object_path(&bucket, &key).parent().unwrap().to_path_buf();
// //     let safe_key = key.replace('/', "_");
// //     let meta_marker = parent.join(format!(".s3meta.{}", safe_key));

// //     // 2. Load bucket meta
// //     let bucket_meta_path = bucket_path.join(".s3meta");
// //     let bucket_meta = load_bucket_meta(&bucket_meta_path).await?;

// //     let versioning = bucket_meta.versioning;

// //     // 3. Load object meta
// //     let mut obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
// //         .ok()
// //         .flatten()
// //         .and_then(|b| String::from_utf8(b).ok())
// //         .and_then(|s| serde_json::from_str(&s).ok())
// //         .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

// //     // 4. If versioning → create delete marker
// //     if versioning {
// //         let next_version = obj_meta.current_version + 1;
// //         let marker_path = parent.join(format!("{}.v{}", key, next_version));

// //         // Create empty file (delete marker)
// //         fs::write(&marker_path, b"").await.context("create delete marker")?;

// //         // Update metadata
// //         obj_meta.current_version = next_version;
// //         obj_meta.version_id = Some(next_version);
// //         obj_meta.is_delete_marker = true;
// //         obj_meta.etag = "\"delete-marker\"".to_string();
// //         obj_meta.content_type = "application/octet-stream".to_string();

// //         let json = serde_json::to_string_pretty(&obj_meta).context("serialize obj meta")?;
// //         set(&meta_marker, "user.s3.meta", json.as_bytes())
// //             .map_err(|e| AppError::Internal(anyhow::anyhow!("save meta: {e}")))?;

// //         // Update bucket stats
// //         let mut bucket_meta = bucket_meta;
// //         bucket_meta.used_bytes += 0; // empty file
// //         save_bucket_meta(&bucket_meta_path, &bucket_meta).await?;

// //         let mut headers = S3Headers::common_headers();
// //         headers.insert("x-amz-version-id", next_version.to_string().parse().unwrap());
// //         headers.insert("x-amz-delete-marker", "true".parse().unwrap());

// //         return Ok((StatusCode::NO_CONTENT, headers));
// //     }

// //     // 5. Non-versioned: actually delete
// //     let base_path = state.object_path(&bucket, &key);
// //     if base_path.exists() {
// //         fs::remove_file(&base_path).await.context("delete file")?;
// //     }

// //     // Remove metadata
// //     if meta_marker.exists() {
// //         fs::remove_file(&meta_marker).await.ok();
// //     }

// //     // Update bucket stats
// //     let size = if base_path.exists() { fs::metadata(&base_path).await?.len() } else { 0 };
// //     let mut bucket_meta = bucket_meta;
// //     bucket_meta.object_count = bucket_meta.object_count.saturating_sub(1);
// //     bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_sub(size);
// //     save_bucket_meta(&bucket_meta_path, &bucket_meta).await?;

// //     Ok((StatusCode::NO_CONTENT, HeaderMap::new()))
// // }