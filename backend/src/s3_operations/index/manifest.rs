
// manifest.rs
use serde::{Serialize, Deserialize};
use anyhow::Result;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    /// LSM levels: levels[0] is L0 (no overlap), then L1, L2, etc.
    pub levels: Vec<Vec<SstMeta>>,

    /// Active WAL file (new writes go here)
    pub current_wal: String,

    /// Global monotonically increasing file ID generator.
    /// This must be persisted and used for both WAL + SST generation.
    pub next_file_id: u64,

    /// Manifest format version.
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstMeta {
    pub file_name: String,
    pub level: u32,
    pub min_key: String,
    pub max_key: String,
    pub size_bytes: u64,
    pub seq_lo: u64,
    pub seq_hi: u64,
    pub entries: u64,
    pub checksum: Option<String>,
}

impl Manifest {
    /// Load manifest or create a new one if missing.
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        if !path.exists() {
            // Create empty manifest with L0 level initialized
            return Ok(Self {
                levels: vec![Vec::new()],
                current_wal: "000001.log".into(),
                next_file_id: 1,
                version: 1,
            });
        }

        let data = fs::read(path).await?;
        let mut mani: Self = serde_json::from_slice(&data)?;

        // Ensure at least L0 exists
        if mani.levels.is_empty() {
            mani.levels.push(Vec::new());
        }

        Ok(mani)
    }

    /// Save manifest through a safe, crash-consistent atomic rename.
    pub async fn atomic_save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let tmp_path = path.with_extension("tmp");

        fs::create_dir_all(path.parent().unwrap()).await?;

        // Write new manifest to temporary file
        let mut f = fs::File::create(&tmp_path).await?;
        let buf = serde_json::to_vec_pretty(self)?;

        f.write_all(&buf).await?;
        f.sync_data().await?;

        fs::rename(&tmp_path, &path).await?;

        // Fsync parent dir to ensure rename persists
        #[cfg(unix)]
        {
            let parent = path.parent().unwrap();
            tokio::task::block_in_place(|| {
                if let Ok(d) = std::fs::File::open(parent) {
                    let _ = d.sync_all();
                }
            });
        }

        Ok(())
    }

    /// Generate next SST file name using global monotonically increasing ID.
    pub fn next_sst_name(&mut self) -> String {
        self.next_file_id += 1;
        format!("{:06}.sst", self.next_file_id)
    }

    /// Add an SST file to the appropriate LSM level.
    pub fn add_file(&mut self, meta: SstMeta) {
        while self.levels.len() <= meta.level as usize {
            self.levels.push(Vec::new());
        }
        self.levels[meta.level as usize].push(meta);
    }

    /// Remove SST files by name from all levels.
    pub fn remove_files(&mut self, names: &[String]) {
        for level in &mut self.levels {
            level.retain(|m| !names.contains(&m.file_name));
        }
    }

    /// Create a new WAL filename (rotated WAL).
    pub fn next_wal_name(&mut self) -> String {
        self.next_file_id += 1;
        self.current_wal = format!("{:06}.log", self.next_file_id);
        self.current_wal.clone()
    }
}


// // manifest.rs
// use serde::{Serialize, Deserialize};
// use anyhow::Result;
// use std::path::Path;
// use tokio::fs;
// use tokio::io::AsyncWriteExt;

// #[derive(Debug, Serialize, Deserialize)]
// pub struct Manifest {
//     pub levels: Vec<Vec<SstMeta>>, // levels[0] is L0
//     pub current_wal: String,
//     pub next_file_id: u64,
//     pub version: u64,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct SstMeta {
//     pub file_name: String,
//     pub level: u32,
//     pub min_key: String,
//     pub max_key: String,
//     pub size_bytes: u64,
//     pub seq_lo: u64,
//     pub seq_hi: u64,
//     pub entries: u64,
//     pub checksum: Option<String>,
// }

// impl Manifest {
//     pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
//         if !path.as_ref().exists() {
//             // Initialize with a starting WAL and empty levels
//             return Ok(Self {
//                 levels: vec![Vec::new()],
//                 current_wal: "000001.log".into(),
//                 next_file_id: 1,
//                 version: 1,
//             });
//         }
//         let data = fs::read(path.as_ref()).await?;
//         Ok(serde_json::from_slice(&data)?)
//     }

//     pub async fn atomic_save(&self, path: impl AsRef<Path>) -> Result<()> {
//         let path = path.as_ref();
//         let tmp = path.with_extension("tmp");
//         fs::create_dir_all(path.parent().unwrap()).await?;
//         let mut f = fs::File::create(&tmp).await?;
//         let buf = serde_json::to_vec_pretty(self)?;
//         f.write_all(&buf).await?;
//         f.sync_data().await?;
//         fs::rename(&tmp, path).await?;
//         // fsync parent directory to persist rename
//         #[cfg(unix)]
//         {
//             let dir = path.parent().unwrap();
//             tokio::task::block_in_place(|| {
//                 let d = std::fs::File::open(dir).unwrap();
//                 let _ = d.sync_all();
//             });
//         }
//         Ok(())
//     }

//     pub fn next_sst_name(&mut self) -> String {
//         self.next_file_id += 1;
//         format!("{:06}.sst", self.next_file_id)
//     }

//     pub fn add_file(&mut self, meta: SstMeta) {
//         while self.levels.len() <= meta.level as usize {
//             self.levels.push(Vec::new());
//         }
//         self.levels[meta.level as usize].push(meta);
//     }

//     pub fn remove_files(&mut self, names: &[String]) {
//         for level in &mut self.levels {
//             level.retain(|m| !names.iter().any(|n| n == &m.file_name));
//         }
//     }

//     // Generate the next WAL filename using the same monotonic counter
//     pub fn next_wal_name(&mut self) -> String {
//         self.next_file_id += 1;
//         format!("{:06}.log", self.next_file_id)
//     }
// }
