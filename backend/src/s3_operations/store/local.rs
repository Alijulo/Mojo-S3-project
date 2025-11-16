// store/local.rs
use super::{ObjectStore, PutOptions, GetOptions, GetResult};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use std::{path::PathBuf, pin::Pin};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.root.join(bucket).join(key)
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
    ) -> Result<()> {
        let path = self.object_path(bucket, key);
        let parent = path.parent().context("Invalid object path")?;
        tokio::fs::create_dir_all(parent).await?;

        // Write to a temporary file first, then atomically rename
        let tmp = path.with_extension("tmp");
        let file = tokio::fs::File::create(&tmp)
            .await
            .context("Failed to create temp file")?;
        if let Some(len) = opts.content_length {
            file.set_len(len).await?;
        }
        let mut w = BufWriter::with_capacity(256 * 1024, file);

        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            w.write_all(&chunk).await?;
        }

        w.flush().await?;
        let file = w.into_inner();  // Fixed: synchronous unwrap, no await or ?
        file.sync_data().await?;
        file.sync_all().await?;
        tokio::fs::rename(&tmp, &path)
            .await
            .context("Failed to rename temp file")?;
        Ok(())
    }

    async fn get_stream(
        &self,
        bucket: &str,
        key: &str,
        _opts: GetOptions,
    ) -> Result<GetResult> {
        let path = self.object_path(bucket, key);
        let mut file = tokio::fs::File::open(&path)
            .await
            .context("Object not found")?;
        let meta = file.metadata().await?;
        let total = meta.len();

        let stream = async_stream::try_stream! {
            const CHUNK: usize = 4 * 1024 * 1024;
            let mut buf = vec![0u8; CHUNK];
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 { break; }
                yield Bytes::copy_from_slice(&buf[..n]);
            }
        };

        Ok(GetResult {
            content_length: total,
            content_type: "application/octet-stream".to_string(),
            etag: format!("\"{}-{}\"", bucket, key), // simple deterministic etag
            version_id: None,
            body: Box::pin(stream),
        })
    }

    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let bucket_path = self.root.join(bucket);
        let mut results = Vec::new();
        if !tokio::fs::metadata(&bucket_path).await.is_ok() {
            return Ok(results);
        }

        let mut dirs = vec![bucket_path.clone()];
        while let Some(dir) = dirs.pop() {
            let mut entries = tokio::fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if let Ok(rel) = path.strip_prefix(&bucket_path) {
                    let key = rel.to_string_lossy().replace('\\', "/");
                    if key.starts_with(prefix) {
                        results.push(key);
                    }
                }
            }
        }
        results.sort();
        Ok(results)
    }

    async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        let path = self.object_path(bucket, key);
        if tokio::fs::metadata(&path).await.is_ok() {
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn exists(&self, bucket: &str, key: &str) -> Result<bool> {
        Ok(tokio::fs::metadata(self.object_path(bucket, key)).await.is_ok())
    }
}