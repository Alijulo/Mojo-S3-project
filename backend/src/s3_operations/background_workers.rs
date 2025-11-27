use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
    time::interval,
};
use serde_json;
use tracing::{debug, error, info};

use crate::{s3_operations::{bucket_handlers::BucketMeta}, DurabilityLevel,};

#[derive(Clone, Debug)]
pub struct BackgroundWorkers {
    pub bucket_meta_tx: Sender<(String, BucketMeta)>,
    pub fsync_tx: Sender<PathBuf>,
    pub archive_copy_tx: Sender<(PathBuf, PathBuf)>,
}

/// Spawns a high-throughput bucket meta worker
pub fn spawn_bucket_meta_worker(
    mut rx: Receiver<(String, BucketMeta)>,
    root: PathBuf,
    durability: DurabilityLevel,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut pending: HashMap<String, BucketMeta> = HashMap::new();
        let mut ticker = interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                Some((bucket, meta)) = rx.recv() => {
                    pending.insert(bucket, meta);
                }
                _ = ticker.tick() => {
                    if !pending.is_empty() {
                        // Flush all pending bucket meta
                        for (bucket, meta) in pending.drain() {
                            let bucket_path = root.join(&bucket);
                            let bucket_meta_path = bucket_path.join(".s3meta");
                            let tmp_path = bucket_meta_path.with_extension("meta.tmp");

                            if let Err(e) = fs::create_dir_all(&bucket_path).await {
                                error!(bucket=%bucket, error=%e, "Failed to create bucket dir");
                                continue;
                            }

                            match fs::File::create(&tmp_path).await {
                                Ok(f) => {
                                    let mut w = tokio::io::BufWriter::new(f);
                                    match serde_json::to_string_pretty(&meta) {
                                        Ok(json) => {
                                            if let Err(e) = w.write_all(json.as_bytes()).await {
                                                error!(bucket=%bucket, error=%e, "Failed to write bucket meta");
                                                continue;
                                            }
                                            if let Err(e) = w.flush().await {
                                                error!(bucket=%bucket, error=%e, "Failed to flush bucket meta");
                                                continue;
                                            }
                                            if durability == DurabilityLevel::Strong {
                                                if let Err(e) = w.get_mut().sync_all().await {
                                                    error!(bucket=%bucket, error=%e, "Failed to fsync bucket meta");
                                                    continue;
                                                }
                                            }
                                            if let Err(e) = fs::rename(&tmp_path, &bucket_meta_path).await {
                                                error!(bucket=%bucket, error=%e, "Failed to rename bucket meta tmp");
                                            } else {
                                                debug!(bucket=%bucket, "Bucket meta updated");
                                            }
                                        }
                                        Err(e) => error!(bucket=%bucket, error=%e, "Failed to serialize bucket meta"),
                                    }
                                }
                                Err(e) => error!(bucket=%bucket, error=%e, "Failed to create bucket meta tmp file"),
                            }
                        }
                    }
                }
            }
        }
    })
}

/// FSYNC worker for directories
pub fn spawn_fsync_worker(mut rx: Receiver<PathBuf>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(path) = rx.recv().await {
            #[cfg(unix)]
            if let Ok(df) = fs::File::open(&path).await {
                let _ = df.sync_all().await;
            }
            #[cfg(windows)]
            {
                debug!(dir=?path, "Windows fsync best-effort");
            }
        }
    })
}

/// Deferred archive copy worker
pub fn spawn_archive_copy_worker(mut rx: Receiver<(PathBuf, PathBuf)>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some((src, dst)) = rx.recv().await {
            if let Err(e) = fs::copy(&src, &dst).await {
                error!(src=?src, dst=?dst, error=%e, "Deferred archive copy failed");
            } else {
                info!(src=?src, dst=?dst, "Deferred archive copy succeeded");
            }
        }
    })
}

