use std::sync::Arc;
use tokio::sync::mpsc;
use anyhow::{Context, Result};
use tracing::{warn, error};
use crate::AppState;
use rand::{rng, Rng};
use tokio::time::{sleep,Duration};
use std::time::{SystemTime};
use std::path::PathBuf;
use crate::s3_operations::index::manifest::Manifest;
use crate::s3_operations::index::compactor::Compactor;
use crate::s3_operations::index::types::WalOp;
use tokio::fs;
// ───────────── Worker types ─────────────
#[derive(Clone, Debug)]
pub enum WorkItem {
    CompactLevel { bucket: String, level: u8 },
    GcSweep { bucket: String },
    RetryCommit { bucket: String, txn_id: String, attempts: u32 },
}




#[derive(Clone)]
pub struct BackgroundWorkers {
    pub tx: mpsc::Sender<WorkItem>,
}

/// Spawn background workers for compaction, GC, and retry
pub fn spawn_workers(state: Arc<AppState>) -> BackgroundWorkers {
    let (tx, mut rx) = mpsc::channel::<WorkItem>(1024);

    // Clone the sender for use inside the worker task
    let tx_clone = tx.clone();

    tokio::spawn({
        let state = Arc::clone(&state);
        async move {
            while let Some(item) = rx.recv().await {
                match item {
                    WorkItem::CompactLevel { bucket, level } => {
                        if let Err(e) = run_compaction(&state, level, &bucket).await {
                            warn!(%bucket, %level, error=?e, "compaction failed; scheduling retry");
                            schedule_retry(&tx_clone, WorkItem::CompactLevel { bucket, level }).await;
                        }
                    }

                    WorkItem::GcSweep { bucket } => {
                        if let Err(e) = run_gc_sweep(&state, &bucket).await {
                            warn!(%bucket, error=?e, "gc sweep failed; scheduling retry");
                            schedule_retry(&tx_clone, WorkItem::GcSweep { bucket }).await;
                        }
                    }
                    WorkItem::RetryCommit { bucket, txn_id, attempts } => {
                        if let Err(e) = retry_commit(&state, &bucket, &txn_id).await {
                            if attempts < 8 {
                                let next = WorkItem::RetryCommit { bucket: bucket.clone(), txn_id: txn_id.clone(),attempts: attempts + 1, };
                                schedule_retry(&tx_clone, next).await;
                            } else {
                                error!(%txn_id, error=?e, "commit retry exhausted");
                            }
                        }
                    }
                }
            }
        }
    });

    BackgroundWorkers { tx }
}


async fn schedule_retry(tx: &mpsc::Sender<WorkItem>, item: WorkItem) {
    // Derive attempt count if present
    let attempts = match &item {
        WorkItem::RetryCommit { attempts, .. } => *attempts,
        _ => 0,
    };

    // Exponential backoff: base delay grows with attempts
    let base_delay = Duration::from_millis(100 * (1 << attempts.min(6))); // cap at ~6.4s
    // Add jitter: random 0–100ms
    let jitter = Duration::from_millis(rng().random_range(0..100));
    let delay = base_delay + jitter;

    tracing::debug!(?item, ?delay, "scheduling retry with backoff");

    sleep(delay).await;

    if let Err(e) = tx.send(item).await {
        tracing::error!(error=?e, "failed to enqueue retry work item");
    }
}

// ───────────── Compaction / GC / Retry stubs ─────────────
pub async fn run_compaction(state: &Arc<AppState>, level: u8, bucket: &str) -> Result<()> {
    // Resolve bucket paths
    let bucket_path: PathBuf = state.storage_root.join(bucket);

    // Initialize compactor
    let compactor = Compactor::new(&bucket_path);

    // Get the bucket index
    let idx = state.get_bucket_index(bucket).await?;

    // Flush memtable into an L0 SST if present
    {
        let mem = idx.mem.clone();
        let mut manifest = idx.manifest.lock().await;
        compactor.flush_memtable(&mem, &mut manifest).await
            .with_context(|| format!("memtable flush failed for bucket {}", bucket))?;
    }

    // Level-specific compaction policy
    match level {
        0 => {
            let mut manifest = idx.manifest.lock().await;
            compactor.compact_levels(&mut manifest).await
                .with_context(|| format!("L0 compaction failed for bucket {}", bucket))?;
        }
        _ => {
            tracing::debug!(%bucket, %level, "no compaction policy defined for this level");
        }
    }

    // Rotate WAL, clear memtable, update cache
    idx.compact().await?;

    tracing::info!(%bucket, %level, "compaction completed successfully");
    Ok(())
}

//TODO implement retention policy
pub async fn run_gc_sweep(state: &Arc<AppState>, bucket: &str) -> Result<()> {
    let bucket_path: PathBuf = state.storage_root.join(bucket);
    let index_path = bucket_path.join("index");
    let wal_path = bucket_path.join("wal");

    // 1) Remove orphan temp files (*.tmp) older than threshold (30 minutes)
    let mut dir = fs::read_dir(&index_path).await
        .with_context(|| format!("failed to read index dir for bucket {}", bucket))?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.extension().map(|e| e == "tmp").unwrap_or(false) {
            let meta = fs::metadata(&path).await?;
            if let Ok(modified) = meta.modified() {
                let age = SystemTime::now().duration_since(modified).unwrap_or_default();
                if age > Duration::from_secs(1800) {
                    tracing::debug!(bucket=%bucket, file=?path, "removing orphan tmp file");
                    let _ = fs::remove_file(&path).await;
                }
            }
        }
    }

    // 2) Apply lifecycle rules (expire versions beyond retention)
    let idx = state.get_bucket_index(bucket).await?;
    {
        let  manifest = idx.manifest.lock().await;
        // TODO: implement retention policy, e.g. manifest.expire_old_versions(Duration::from_secs(7*24*3600)).await?;
    }

    // 3) Delete compacted SSTs no longer referenced
    {
        let manifest = idx.manifest.lock().await;
        let active_files: Vec<String> = manifest.levels
            .iter()
            .flat_map(|lvl| lvl.iter().map(|m| m.file_name.clone()))
            .collect();

        let mut dir = fs::read_dir(&index_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".sst") && !active_files.contains(&name.to_string()) {
                    tracing::debug!(bucket=%bucket, file=?path, "removing obsolete SST");
                    let _ = fs::remove_file(&path).await;
                }
            }
        }
    }

    // 4) Clean up old WAL .log files (older than 1 day, not current WAL)
    {
        let manifest = idx.manifest.lock().await;
        let current_wal = &manifest.current_wal;

        let mut dir = fs::read_dir(&wal_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().map(|e| e == "log").unwrap_or(false) {
                let fname = path.file_name().unwrap().to_string_lossy().to_string();
                if fname != *current_wal {
                    let meta = fs::metadata(&path).await?;
                    if let Ok(modified) = meta.modified() {
                        let age = SystemTime::now().duration_since(modified).unwrap_or_default();
                        if age > Duration::from_secs(24 * 3600) {
                            tracing::debug!(bucket=%bucket, file=?path, "removing obsolete WAL log");
                            let _ = fs::remove_file(&path).await;
                        }
                    }
                }
            }
        }
    }

    tracing::info!(%bucket, "GC sweep completed (tmp, SST, WAL logs)");
    Ok(())
}

pub async fn retry_commit(state: &Arc<AppState>, bucket: &str, txn_id: &str) -> Result<()> {
    // 1) Re‑verify WAL record for this bucket
    let idx = state.get_bucket_index(bucket).await?;
    let mut wal = idx.wal.lock().await;

    // Wal::load should return Option<WalOp>
    let maybe_op = wal.load(txn_id)
        .await
        .with_context(|| format!("failed to load WAL record for txn {} in bucket {}", txn_id, bucket))?;

    let op = match maybe_op {
        Some(op) => op,
        None => return Err(anyhow::anyhow!("no WAL record found for txn {} in bucket {}", txn_id, bucket)),
    };

    match op {
        WalOp::Put { tmp_path, final_path, .. }
        | WalOp::Delete { tmp_path, final_path, .. } => {
            // 2) Verify temp file integrity
            if !fs::try_exists(&tmp_path).await? {
                return Err(anyhow::anyhow!("temp file missing for txn {} in bucket {}", txn_id, bucket));
            }

            // 3) Attempt atomic rename into final location
            fs::rename(&tmp_path, &final_path).await
                .with_context(|| format!("rename failed for txn {} in bucket {}", txn_id, bucket))?;

            // 4) Fsync final file
            let f = fs::File::open(&final_path).await?;
            f.sync_all().await?;

            // 5) Mark WAL record as committed
            wal.mark_committed(txn_id).await?;
        }
        WalOp::Commit { .. } => {
            tracing::info!(%bucket, %txn_id, "txn already committed, skipping retry");
        }
    }

    tracing::info!(%bucket, %txn_id, "retry commit succeeded");
    Ok(())
}

// ───────────── Prefix listing + tombstone handling ─────────────
#[derive(Clone, Debug)]
pub struct KeyEntry {
    pub key: Vec<u8>,       // encoded key
    pub seq_no: u64,        // monotonic sequence
    pub is_tombstone: bool, // delete marker
    pub value_ptr: Option<ValuePtr>, // e.g., offset/len in object meta SST
}

// Placeholder for your value pointer type
#[derive(Clone, Debug)]
pub struct ValuePtr {
    pub offset: u64,
    pub length: u64,
}

pub trait SourceIter {
    fn seek_prefix(&mut self, prefix: &[u8]) -> Result<()>;
    fn current(&self) -> Option<&KeyEntry>;
    fn next(&mut self) -> Result<()>;
}

pub struct MergeIter<'a> {
    // heap keyed by (key, seq_no desc, source index)
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(Vec<u8>, u64, usize)>>,
    iters: Vec<Box<dyn SourceIter + 'a>>,
    last_key: Option<Vec<u8>>,
}

impl<'a> MergeIter<'a> {
    pub fn new(mut iters: Vec<Box<dyn SourceIter + 'a>>, prefix: &[u8]) -> Result<Self> {
        let mut heap = std::collections::BinaryHeap::new();
        for (i, it) in iters.iter_mut().enumerate() {
            it.seek_prefix(prefix)?;
            if let Some(e) = it.current() {
                heap.push(std::cmp::Reverse((e.key.clone(), e.seq_no, i)));
            }
        }
        Ok(Self { heap, iters, last_key: None })
    }

    pub fn next_dedup(&mut self) -> Result<Option<KeyEntry>> {
        // Pop all entries for the smallest key, pick the highest seq_no
        let mut candidates: Vec<(Vec<u8>, u64, usize)> = Vec::new();
        let first = match self.heap.pop() {
            None => return Ok(None),
            Some(std::cmp::Reverse(t)) => t,
        };
        let target_key = first.0.clone();
        candidates.push(first);
        while let Some(std::cmp::Reverse((k, seq, idx))) = self.heap.peek().cloned() {
            if k == target_key {
                self.heap.pop();
                candidates.push((k, seq, idx));
            } else {
                break;
            }
        }

        // Choose highest seq_no for the target_key
        let (best_seq, best_idx) = candidates
            .iter()
            .max_by_key(|(_, seq, _)| *seq)
            .map(|(_, seq, idx)| (*seq, *idx))
            .unwrap();

        // Advance the iterator that produced the chosen entry
        let it = &mut self.iters[best_idx];
        it.next()?;
        if let Some(e) = it.current() {
            self.heap.push(std::cmp::Reverse((e.key.clone(), e.seq_no, best_idx)));
        }

        // Construct final entry: respect tombstones
        // In a real implementation, you'd fetch the actual KeyEntry from the iterator
        Ok(Some(KeyEntry {
            key: target_key,
            seq_no: best_seq,
            is_tombstone: false, // set correctly from iterator
            value_ptr: None,
        }))
    }
}
