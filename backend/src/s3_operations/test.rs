// use axum::{
//     extract::{Path as AxumPath, State},
//     http::{header, HeaderMap, HeaderValue, StatusCode},
//     Extension,
// };
// use bytes::Bytes;
// use futures_util::StreamExt;
// use tokio::{
//     fs,
//     io::{AsyncWriteExt, BufWriter},
// };
// use std::{path::PathBuf, sync::Arc};
// use md5::Context as Md5Context;
// use anyhow::Context;

// use crate::{
//     s3_operations::{
//         auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel},
//         metadata::{get, set},
//         bucket_handlers::BucketMeta,
//     },
//     AppError, AppState, S3Headers,
// };

// use std::sync::atomic::{AtomicU64, Ordering};
// static PUT_BYTES: AtomicU64 = AtomicU64::new(0);
// static PUT_COUNT: AtomicU64 = AtomicU64::new(0);

// #[derive(Debug, Serialize, Deserialize, Default)]
// struct ObjectMeta {
//     #[serde(default)] etag: String,
//     #[serde(default)] content_type: String,
//     #[serde(default)] owner: String,
//     #[serde(default)] current_version: u64,
//     #[serde(default, skip_serializing_if = "Option::is_none")] version_id: Option<u64>,
//     #[serde(default)] is_delete_marker: bool,
// }
// impl ObjectMeta {
//     pub fn new(owner: &str) -> Self {
//         Self {
//             etag: "".to_string(),
//             content_type: "application/octet-stream".to_string(),
//             owner: owner.to_string(),
//             current_version: 0,
//             version_id: None,
//             is_delete_marker: false,
//             ..Default::default()
//         }
//     }
// }

// pub async fn put_object(
//     State(state): State<Arc<AppState>>,
//     AxumPath((bucket, key)): AxumPath<(String, String)>,
//     user: Extension<AuthenticatedUser>,
//     req: axum::extract::Request,
// ) -> Result<(StatusCode, HeaderMap), AppError> {
//     PUT_COUNT.fetch_add(1, Ordering::Relaxed);
//     let start = std::time::Instant::now();

//     // 1) AuthZ
//     let allowed = check_bucket_permission(
//         &state.pool,
//         &user.0,
//         &bucket,
//         PermissionLevel::ReadWrite.as_str(),
//     )
//     .await
//     .map_err(AppError::Internal)?;
//     if !allowed {
//         return Err(AppError::AccessDenied);
//     }

//     // 2) Concurrency controls
//     let _permit = state.io_budget.acquire().await;
//     let lock_key = format!("{bucket}/{key}");
//     let key_lock = state.get_key_lock(&lock_key); // helper on AppState
//     let _guard = key_lock.lock().await;

//     // 3) Paths and parent dir
//     let base_path = state.object_path(&bucket, &key);
//     let bucket_path = state.bucket_path(&bucket);
//     if !bucket_path.exists() {
//         return Err(AppError::NotFound(bucket));
//     }
//     let parent = base_path.parent().unwrap().to_path_buf();
//     fs::create_dir_all(&parent)
//         .await
//         .context("create parent")
//         .map_err(AppError::Internal)?;

//     // 4) Bucket meta (versioning + stats)
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

//     // 5) Object meta sidecar
//     let safe_key = key.replace('/', "_");
//     let meta_marker = parent.join(format!(".s3meta.{safe_key}"));
//     let mut obj_meta: ObjectMeta = get(&meta_marker, "user.s3.meta")
//         .await
//         .ok()
//         .flatten()
//         .and_then(|b| String::from_utf8(b).ok())
//         .and_then(|s| serde_json::from_str(&s).ok())
//         .unwrap_or_else(|| ObjectMeta::new(&user.0.username));

//     // 6) Conditional headers
//     if let Some(if_match) = req.headers().get(header::IF_MATCH).and_then(|h| h.to_str().ok()) {
//         let expected = if_match.trim();
//         if !expected.is_empty() && obj_meta.etag != expected {
//             return Err(AppError::PreconditionFailed("If-Match".into()));
//         }
//     }
//     if let Some(if_none_match) = req.headers().get(header::IF_NONE_MATCH).and_then(|h| h.to_str().ok()) {
//         let forbidden = if_none_match.trim();
//         if !forbidden.is_empty() && obj_meta.etag == forbidden {
//             return Err(AppError::PreconditionFailed("If-None-Match".into()));
//         }
//     }

//     // 7) Final path with versioning
//     let (final_path, version_id, is_new_object) = if versioning {
//         let v = obj_meta.current_version.saturating_add(1);
//         (parent.join(format!("{}.v{}", key, v)), Some(v), v == 1)
//     } else {
//         (base_path.clone(), None, obj_meta.current_version == 0)
//     };

//     // 8) Content headers
//     let content_type = req
//         .headers()
//         .get(header::CONTENT_TYPE)
//         .and_then(|h| h.to_str().ok())
//         .unwrap_or("application/octet-stream")
//         .to_string();
//     let content_len = req
//         .headers()
//         .get(header::CONTENT_LENGTH)
//         .and_then(|h| h.to_str().ok())
//         .and_then(|s| s.parse::<u64>().ok());
//     let content_md5_b64 = req
//         .headers()
//         .get("Content-MD5")
//         .and_then(|h| h.to_str().ok())
//         .map(str::to_string);

//     // 9) Temp write with backpressure, prealloc, fsync
//     let tmp_path = final_path.with_extension("upload.tmp");

//     struct TempGuard {
//         path: PathBuf,
//         committed: bool,
//     }
//     impl Drop for TempGuard {
//         fn drop(&mut self) {
//             if !self.committed {
//                 let _ = std::fs::remove_file(&self.path);
//             }
//         }
//     }
//     let mut guard = TempGuard {
//         path: tmp_path.clone(),
//         committed: false,
//     };

//     let file = fs::File::create(&tmp_path)
//         .await
//         .context("create temp")
//         .map_err(AppError::Internal)?;
//     let mut writer = BufWriter::with_capacity(512 * 1024, file);

//     if let Some(len) = content_len {
//         let _ = writer.get_mut().set_len(len).await;
//     }

//     let mut hasher = Md5Context::new();
//     let mut total_size = 0u64;
//     let mut body_stream = req.into_body().into_data_stream();

//     while let Some(chunk) = body_stream.next().await {
//         let chunk = chunk.context("read chunk").map_err(AppError::Internal)?;
//         let bytes = chunk.as_ref();
//         hasher.consume(bytes);
//         writer
//             .write_all(bytes)
//             .await
//             .context("write chunk")
//             .map_err(AppError::Internal)?;
//         total_size += bytes.len() as u64;
//     }

//     writer.flush().await.context("flush").map_err(AppError::Internal)?;
//     writer
//         .get_mut()
//         .sync_data()
//         .await
//         .context("sync_data")
//         .map_err(AppError::Internal)?;
//     writer
//         .get_mut()
//         .sync_all()
//         .await
//         .context("sync_all")
//         .map_err(AppError::Internal)?;

//     // 10) ETag + Content-MD5 validation
//     let digest = hasher.finalize();
//     let etag = format!("\"{}\"", hex::encode(digest.0));
//     if let Some(md5_b64) = content_md5_b64 {
//         let computed_b64 = base64::encode(digest.0);
//         if computed_b64 != md5_b64 {
//             return Err(AppError::PreconditionFailed("Content-MD5".into()));
//         }
//     }

//     // 11) Atomic commit
//     fs::rename(&tmp_path, &final_path)
//         .await
//         .context("rename temp->final")
//         .map_err(AppError::Internal)?;
//     guard.committed = true;

//     // 12) Update metadata
//     obj_meta.etag = etag.clone();
//     obj_meta.content_type = content_type;
//     obj_meta.owner = user.0.username.clone();
//     obj_meta.is_delete_marker = false;

//     if let Some(v) = version_id {
//         obj_meta.current_version = v;
//         obj_meta.version_id = Some(v);
//     } else {
//         obj_meta.current_version = 1;
//         obj_meta.version_id = None;
//     }

//     let obj_json = serde_json::to_string_pretty(&obj_meta)
//         .context("serialize obj meta")
//         .map_err(AppError::Internal)?;
//     set(&meta_marker, "user.s3.meta", obj_json.as_bytes())
//         .await
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("set obj meta: {e}")))?;

//     // 13) Update bucket stats
//     if is_new_object {
//         bucket_meta.object_count = bucket_meta.object_count.saturating_add(1);
//     }
//     bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(total_size);

//     let bucket_json = serde_json::to_string_pretty(&bucket_meta)
//         .context("serialize bucket meta")
//         .map_err(AppError::Internal)?;
//     set(&bucket_meta_path, "user.s3.meta", bucket_json.as_bytes())
//         .await
//         .map_err(|e| AppError::Internal(anyhow::anyhow!("set bucket meta: {e}")))?;

//     // 14) Metrics + response
//     PUT_BYTES.fetch_add(total_size, Ordering::Relaxed);
//     tracing::info!(
//         "PUT {}/{} | {} bytes | {} ms | ETag: {} | v={:?}",
//         bucket,
//         key,
//         total_size,
//         start.elapsed().as_millis(),
//         etag,
//         version_id
//     );

//     let mut headers = S3Headers::common_headers();
//     headers.insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
//     headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
//     if let Some(v) = version_id {
//         headers.insert("x-amz-version-id", HeaderValue::from_str(&v.to_string()).unwrap());
//     }
//     Ok((StatusCode::OK, headers))
// }
