use super::{ObjectStore, PutOptions, GetOptions, GetResult};
use anyhow::{anyhow,Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use std::{path::PathBuf, pin::Pin, sync::Arc};
use tokio::fs;
use tokio::io::BufWriter;
use std::sync::atomic::{AtomicU64, Ordering,AtomicBool};

use crate::{s3_operations::{handler_utils::AppError,object_handlers::{ObjectMeta, load_object_meta, archive_previous_version, commit_object_transaction,encode_token,decode_token,TempGuard,ChecksumWriter}, 
bucket_handlers::load_bucket_meta}, AppState,DurabilityLevel,};
use tokio::io::AsyncWriteExt;

pub trait DurableOps {
    fn durability_enabled(&self) -> bool;
}

impl DurableOps for AppState {
    fn durability_enabled(&self) -> bool {
        self.durability == DurabilityLevel::Strong
    }
}
static PUT_BYTES: AtomicU64 = AtomicU64::new(0);
static PUT_COUNT: AtomicU64 = AtomicU64::new(0);

pub struct LocalStore {
    root: PathBuf,
//    state: Arc<AppState>,
}

impl LocalStore {
    // pub fn new(root: impl Into<PathBuf>, state: Arc<AppState>) -> Self {
    //     Self { root: root.into(), state }
    // }
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }


    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.root.join(bucket).join(key)
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.root.join(bucket)
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_stream(
    &self,
    bucket: &str,
    key: &str,
    opts: PutOptions,
    mut body: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
    state: Arc<AppState>,
    ) -> Result<(String, u64, Option<String>), AppError> {

        // ────────────────────────────────
        // SECTION 0: Paths + Bucket Meta
        // ────────────────────────────────
        let base_path = self.object_path(bucket, key);
        let bucket_path = self.bucket_path(bucket);
        fs::create_dir_all(&bucket_path).await?;
        let parent = base_path.parent().context("Invalid object path")?;
        fs::create_dir_all(parent).await?;

        let mut bucket_meta = load_bucket_meta(&bucket_path).await?;
        let versioning = opts.versioning;

        // ────────────────────────────────
        // SECTION 1: Safe Key + Object Meta
        // ────────────────────────────────
        let safe_key_base = encode_token(key);
        let safe_key = if safe_key_base.len() <= 200 {
            safe_key_base
        } else {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(key.as_bytes());
            format!("sha256-{}", hex::encode(hasher.finalize()))
        };

        let mut obj_meta = load_object_meta(parent, &safe_key)
            .await
            .unwrap_or_else(|_| ObjectMeta::new("system"));

        obj_meta.set_content_type(opts.content_type.clone().unwrap_or("application/octet-stream".to_string()));
        obj_meta.set_tags(opts.tags.clone().unwrap_or_default());
        obj_meta.set_user_meta(opts.user_meta.clone().unwrap_or_default());

        // ────────────────────────────────
        // SECTION 2: Existence + Conditionals
        // ────────────────────────────────
        let meta_opt = fs::metadata(&base_path).await.ok();
        let existed = meta_opt.is_some();
        let old_size = meta_opt.map(|m| m.len()).unwrap_or(0);
        let current_etag_unquoted = obj_meta.etag().trim().trim_matches('"');

        if let Some(if_match) = opts.if_match.as_ref() {
            let expected = if_match.trim();
            if expected == "*" {
                if !existed {
                    return Err(AppError::PreconditionFailed);
                }
            } else {
                let expected_unquoted = expected.trim_matches('"');
                if current_etag_unquoted.is_empty() || current_etag_unquoted != expected_unquoted {
                    return Err(AppError::PreconditionFailed);
                }
            }
        }
        if let Some(if_none_match) = opts.if_none_match.as_ref() {
            let forbidden = if_none_match.trim();
            if forbidden == "*" {
                if existed {
                    return Err(AppError::PreconditionFailed);
                }
            } else {
                let forbidden_unquoted = forbidden.trim_matches('"');
                if existed && current_etag_unquoted == forbidden_unquoted {
                    return Err(AppError::PreconditionFailed);
                }
            }
        }

        // ────────────────────────────────
        // SECTION 3: Versioning + Archive
        // ────────────────────────────────
        let new_version_id = if versioning { Some(uuid::Uuid::new_v4().to_string()) } else { None };
        if versioning && existed {
            let prev_version_id = obj_meta.version_id().map(|s| s.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            archive_previous_version(&state, &base_path, parent, &safe_key, &prev_version_id).await?;
        }

        // ────────────────────────────────
        // SECTION 4: Temp File + Streaming Write
        // ────────────────────────────────
        let tmp_path = parent.join(format!("{}.{}.tmp", safe_key, uuid::Uuid::new_v4()));
        let file = fs::File::create(&tmp_path).await?;
        let guard = TempGuard::new(tmp_path.clone());
        let inner_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
        let mut writer = ChecksumWriter::new(inner_writer);

        let mut total_size = 0u64;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            writer.write_all(&chunk).await?;
            total_size += chunk.len() as u64;
        }

        if let Some(expected_len) = opts.content_length {
            if total_size != expected_len {
                guard.cleanup_async().await;
                return Err(AppError::BadRequest("Content-Length mismatch".into()));
            }
        }

        // ────────────────────────────────
        // SECTION 5: Flush + Checksums
        // ────────────────────────────────
        if state.durability_enabled() {
            writer.flush_and_sync().await?;
        } else {
            writer.flush().await?;
        }
        let (md5_bytes, sha256_bytes, _) = writer.finalize();
        let etag = format!("\"{}\"", hex::encode(&md5_bytes));

        if let Some(expected_etag) = opts.etag.as_ref() {
            if &etag != expected_etag {
                guard.cleanup_async().await;
                return Err(AppError::BadDigest("ETag mismatch".into()));
            }
        }

        obj_meta.set_etag(etag.clone());
        obj_meta.set_version_id(new_version_id.clone());
        obj_meta.set_owner("system");
        obj_meta.set_delete_marker(false);

        // ────────────────────────────────
        // SECTION 6: Bucket meta deltas
        // ────────────────────────────────
        if !existed {
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
        // SECTION 7: WAL Index + Commit
        // ────────────────────────────────
        let bucket_index = state.get_bucket_index(bucket).await?;
        let txn_id = uuid::Uuid::new_v4().to_string();
        commit_object_transaction(
            &state,
            txn_id,
            &tmp_path,
            &base_path,
            parent,
            &safe_key,
            &obj_meta,
            &bucket_path,
            &bucket_meta,
            &bucket_index,
            total_size,
            key,
        ).await?;

        // ────────────────────────────────
        // SECTION 8: Finalize
        // ────────────────────────────────
        PUT_BYTES.fetch_add(total_size, std::sync::atomic::Ordering::Relaxed);
        guard.mark_committed();

        Ok((etag, total_size, new_version_id))
    }

    // async fn put_stream(
    //     &self,
    //     bucket: &str,
    //     key: &str,
    //     opts: PutOptions,
    //     mut body: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
    // ) -> Result<()> {
    //     // ────────────────────────────────
    //     // SECTION 0: Metrics + Validation
    //     // ────────────────────────────────
    //     PUT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    //     let start = std::time::Instant::now();

    //     let base_path = self.object_path(bucket, key);
    //     let bucket_path = self.bucket_path(bucket);
    //     fs::create_dir_all(&bucket_path).await?;
    //     let parent = base_path.parent().context("Invalid object path")?;
    //     fs::create_dir_all(parent).await?;

    //     let mut bucket_meta = load_bucket_meta(&bucket_path).await?;
    //     let versioning = bucket_meta.versioning;

    //     // ────────────────────────────────
    //     // SECTION 1: Safe Key + Object Meta
    //     // ────────────────────────────────
    //     let safe_key_base = encode_token(key);
    //     let safe_key = if safe_key_base.len() <= 200 {
    //         safe_key_base
    //     } else {
    //         use sha2::{Digest, Sha256};
    //         let mut hasher = Sha256::new();
    //         hasher.update(key.as_bytes());
    //         format!("sha256-{}", hex::encode(hasher.finalize()))
    //     };

    //     let mut obj_meta = load_object_meta(parent, &safe_key)
    //         .await
    //         .unwrap_or_else(|_| ObjectMeta::new("system"));

    //     obj_meta.set_content_type(opts.content_type.clone().unwrap_or("application/octet-stream".to_string()));
    //     obj_meta.set_tags(opts.tags.clone().unwrap_or_default());
    //     obj_meta.set_user_meta(opts.user_meta.clone().unwrap_or_default());

    //     // ────────────────────────────────
    //     // SECTION 2: Existence + Conditional Headers
    //     // ────────────────────────────────
    //     let meta_opt = fs::metadata(&base_path).await.ok();
    //     let existed = meta_opt.is_some();
    //     let old_size = meta_opt.map(|m| m.len()).unwrap_or(0);
    //     let is_new_object = !existed;

    //     let current_etag_unquoted = obj_meta.etag().trim().trim_matches('"');

    //     if let Some(if_match) = opts.if_match.as_ref() {
    //         let expected = if_match.trim();
    //         if expected == "*" {
    //             if !existed {
    //                 return Err(anyhow!("PreconditionFailed: If-Match * but object does not exist"));
    //             }
    //         } else {
    //             let expected_unquoted = expected.trim_matches('"');
    //             if current_etag_unquoted.is_empty() || current_etag_unquoted != expected_unquoted {
    //                 return Err(anyhow!("PreconditionFailed: ETag mismatch"));
    //             }
    //         }
    //     }

    //     if let Some(if_none_match) = opts.if_none_match.as_ref() {
    //         let forbidden = if_none_match.trim();
    //         if forbidden == "*" {
    //             if existed {
    //                 return Err(anyhow!("PreconditionFailed: If-None-Match * but object exists"));
    //             }
    //         } else {
    //             let forbidden_unquoted = forbidden.trim_matches('"');
    //             if existed && current_etag_unquoted == forbidden_unquoted {
    //                 return Err(anyhow!("PreconditionFailed: ETag matched forbidden value"));
    //             }
    //         }
    //     }

    //     // ────────────────────────────────
    //     // SECTION 3: Versioning + Archive
    //     // ────────────────────────────────
    //     let new_version_id = if versioning { Some(uuid::Uuid::new_v4().to_string()) } else { None };
    //     if versioning && existed {
    //         let prev_version_id = obj_meta.version_id().map(|s| s.to_string())
    //             .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    //         archive_previous_version(&self.state, &base_path, parent, &safe_key, &prev_version_id).await?;
    //     }

    //     // ────────────────────────────────
    //     // SECTION 4: Temp File + Streaming Write
    //     // ────────────────────────────────
    //     let tmp_path = parent.join(format!("{}.{}.tmp", safe_key, uuid::Uuid::new_v4()));
    //     let file = fs::File::create(&tmp_path).await?;
    //     let guard = TempGuard::new(tmp_path.clone());
    //     let inner_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    //     let mut writer = ChecksumWriter::new(inner_writer);

    //     let mut total_size = 0u64;
    //     while let Some(chunk) = body.next().await {
    //         let chunk = chunk?;
    //         use tokio::io::AsyncWriteExt;
    //         writer.write_all(&chunk).await?;
    //         total_size += chunk.len() as u64;
    //     }

    //     if let Some(expected_len) = opts.content_length {
    //         if total_size != expected_len {
    //             guard.cleanup_async().await;
    //             return Err(anyhow!("Content-Length mismatch"));
    //         }
    //     }

    //     // ────────────────────────────────
    //     // SECTION 5: Flush + Checksums
    //     // ────────────────────────────────
    //     if self.state.durability_enabled() {
    //         writer.flush_and_sync().await?;
    //     } else {
    //         writer.flush().await?;
    //     }
    //     let (md5_bytes, sha256_bytes, _) = writer.finalize();
    //     let etag = format!("\"{}\"", hex::encode(&md5_bytes));

    //     obj_meta.set_etag(etag.clone());
    //     obj_meta.set_version_id(new_version_id.clone());

    //     // ────────────────────────────────
    //     // SECTION 6: Prepare metadata + bucket meta deltas
    //     // ────────────────────────────────
    //     obj_meta.set_owner("system");
    //     obj_meta.set_delete_marker(false);

    //     if is_new_object {
    //         bucket_meta.object_count = bucket_meta.object_count.saturating_add(1);
    //     }
    //     if versioning {
    //         bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(total_size);
    //     } else {
    //         if total_size >= old_size {
    //             bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_add(total_size - old_size);
    //         } else {
    //             bucket_meta.used_bytes = bucket_meta.used_bytes.saturating_sub(old_size - total_size);
    //         }
    //     }

    //     // ────────────────────────────────
    //     // SECTION 7: WAL Index + Commit
    //     // ────────────────────────────────
    //     let bucket_index = self.state.get_bucket_index(bucket).await?;
    //     // Generate a unique transaction ID 
    //     let txn_id = Uuid::new_v4().to_string();
    //     commit_object_transaction(
    //         &self.state,
    //         txn_id,
    //         &tmp_path,
    //         &base_path,
    //         parent,
    //         &safe_key,
    //         &obj_meta,
    //         &bucket_path,
    //         &bucket_meta,
    //         &bucket_index,
    //         total_size,
    //         key,
    //     ).await?;
    //     // ────────────────────────────────
    //     // SECTION 8: Metrics + Finalize
    //     // ────────────────────────────────
    //     PUT_BYTES.fetch_add(total_size, std::sync::atomic::Ordering::Relaxed);
    //     guard.mark_committed();

    //     tracing::info!(
    //         bucket=%bucket,
    //         key=%key,
    //         bytes=%total_size,
    //         ms=%start.elapsed().as_millis(),
    //         etag=%etag,
    //         version_id=?new_version_id,
    //         "LocalStore PUT commit success"
    //     );

    //     Ok(())
    // }
    async fn get_stream(
            &self,
            bucket: &str,
            key: &str,
            _opts: GetOptions,
        ) -> Result<GetResult> {
            let path = self.object_path(bucket, key);
            let file = fs::File::open(&path)
                .await
                .context("Object not found")?;
            let meta = file.metadata().await?;
            let total = meta.len();

            let stream = async_stream::try_stream! {
                const CHUNK: usize = 4 * 1024 * 1024;
                let mut reader = tokio::io::BufReader::new(file);
                let mut buf = vec![0u8; CHUNK];
                loop {
                    let n = reader.read(&mut buf).await?;
                    if n == 0 {
                        break; // EOF
                    }
                    yield Bytes::copy_from_slice(&buf[..n]);
                }
            };

            Ok(GetResult {
                content_length: total,
                content_type: "application/octet-stream".to_string(),
                etag: format!("\"{}-{}\"", bucket, key),
                version_id: None,
                body: Box::pin(stream),
            })
    }
}






// // store/local.rs
// use super::{ObjectStore, PutOptions, GetOptions, GetResult};
// use anyhow::{Context, Result};
// use async_trait::async_trait;
// use bytes::Bytes;
// use futures_core::Stream;
// use futures_util::StreamExt;
// use std::{path::PathBuf, pin::Pin};
// use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter, BufReader};



// pub struct LocalStore {
//     root: PathBuf,
// }

// impl LocalStore {
//     pub fn new(root: impl Into<PathBuf>) -> Self {
//         Self { root: root.into() }
//     }

//     fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
//         self.root.join(bucket).join(key)
//     }
// }

// #[async_trait]
// impl ObjectStore for LocalStore {
//     async fn put_stream(
//         &self,
//         bucket: &str,
//         key: &str,
//         opts: PutOptions,
//         mut body: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
//     ) -> Result<()> {
//         let path = self.object_path(bucket, key);
//         let parent = path.parent().context("Invalid object path")?;
//         tokio::fs::create_dir_all(parent).await?;

//         // Write to a temporary file first, then atomically rename
//         let tmp = path.with_extension("tmp");
//         let file = tokio::fs::File::create(&tmp)
//             .await
//             .context("Failed to create temp file")?;
//         if let Some(len) = opts.content_length {
//             file.set_len(len).await?;
//         }
//         let mut w = BufWriter::with_capacity(256 * 1024, file);

//         while let Some(chunk) = body.next().await {
//             let chunk = chunk?;
//             w.write_all(&chunk).await?;
//         }

//         w.flush().await?;
//         let file = w.into_inner();  // Fixed: synchronous unwrap, no await or ?
//         file.sync_data().await?;
//         file.sync_all().await?;
//         tokio::fs::rename(&tmp, &path)
//             .await
//             .context("Failed to rename temp file")?;
//         Ok(())
//     }


//     async fn get_stream(
//         &self,
//         bucket: &str,
//         key: &str,
//         _opts: GetOptions,
//     ) -> Result<GetResult> {
//         let path = self.object_path(bucket, key);
//         let file = tokio::fs::File::open(&path)
//             .await
//             .context("Object not found")?;
//         let meta = file.metadata().await?;
//         let total = meta.len();

//         let stream = async_stream::try_stream! {
//             const CHUNK: usize = 4 * 1024 * 1024;
//             let mut reader = BufReader::new(file);
//             let mut buf = vec![0u8; CHUNK];
//             loop {
//                 let n = reader.read(&mut buf).await?;
//                 if n == 0 {
//                     break; // EOF
//                 }
//                 yield Bytes::copy_from_slice(&buf[..n]);
//             }
//         };

//         Ok(GetResult {
//             content_length: total,
//             content_type: "application/octet-stream".to_string(),
//             etag: format!("\"{}-{}\"", bucket, key),
//             version_id: None,
//             body: Box::pin(stream),
//         })
//     }

//     async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
//         let bucket_path = self.root.join(bucket);
//         let mut results = Vec::new();
//         if !tokio::fs::metadata(&bucket_path).await.is_ok() {
//             return Ok(results);
//         }

//         let mut dirs = vec![bucket_path.clone()];
//         while let Some(dir) = dirs.pop() {
//             let mut entries = tokio::fs::read_dir(&dir).await?;
//             while let Some(entry) = entries.next_entry().await? {
//                 let path = entry.path();
//                 if path.is_dir() {
//                     dirs.push(path);
//                     continue;
//                 }
//                 if let Ok(rel) = path.strip_prefix(&bucket_path) {
//                     let key = rel.to_string_lossy().replace('\\', "/");
//                     if key.starts_with(prefix) {
//                         results.push(key);
//                     }
//                 }
//             }
//         }
//         results.sort();
//         Ok(results)
//     }

//     async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
//         let path = self.object_path(bucket, key);
//         if tokio::fs::metadata(&path).await.is_ok() {
//             tokio::fs::remove_file(&path).await?;
//         }
//         Ok(())
//     }

//     async fn exists(&self, bucket: &str, key: &str) -> Result<bool> {
//         Ok(tokio::fs::metadata(self.object_path(bucket, key)).await.is_ok())
//     }
// }