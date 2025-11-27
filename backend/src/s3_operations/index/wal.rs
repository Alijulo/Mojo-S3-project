// //wal.rs

use crate::index::types::WalOp;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufWriter};
use anyhow::{Result, Context};
use std::path::{Path, PathBuf};

pub struct Wal {
    writer: BufWriter<File>,
    pub path: PathBuf,
}

impl Wal {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;

        let file = File::options()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .context("failed to open WAL file")?;

        Ok(Self { writer: BufWriter::new(file), path })
    }

    pub async fn append(&mut self, op: &WalOp) -> Result<()> {
        let bytes = serde_json::to_vec(op)?;

        // Prepare frame: [u32_le][payload]
        let mut frame = Vec::with_capacity(4 + bytes.len());
        frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        frame.extend_from_slice(&bytes);

        // One write → fewer syscalls
        self.writer.write_all(&frame).await
            .context("failed to write WAL frame")?;

        self.writer.flush().await?;
        self.writer.get_mut().sync_data().await?;

        Ok(())
    }

    pub async fn replay(path: impl AsRef<Path>) -> Result<Vec<WalOp>> {
        let mut ops = Vec::new();
        let path = path.as_ref();

        if !path.exists() {
            return Ok(ops);
        }

        let mut file = File::open(path)
            .await
            .context("failed to open WAL for replay")?;

        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => (),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // clean EOF
                Err(e) => {
                    return Err(e).context("WAL corruption: failed reading record length");
                }
            }

            let len = u32::from_le_bytes(len_buf);
            let mut buf = vec![0u8; len as usize];

            match file.read_exact(&mut buf).await {
                Ok(_) => (),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // truncated payload → WAL corruption
                    return Err(e).context("WAL corruption: incomplete record payload");
                }
                Err(e) => {
                    return Err(e).context("WAL corruption: failed reading record payload");
                }
            }

            let op: WalOp = serde_json::from_slice(&buf)
                .context("failed to deserialize WAL record")?;

            ops.push(op);
        }

        Ok(ops)
    }
}


// use crate::index::types::WalOp;
// use tokio::fs::File;
// use tokio::io::{AsyncWriteExt, AsyncReadExt, BufWriter};
// use anyhow::{Result, Context};
// use std::path::{Path, PathBuf};

// pub struct Wal {
//     writer: BufWriter<File>,
//     pub path: PathBuf,
// }

// impl Wal {
//     pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
//         let path = path.as_ref().to_path_buf();
//         tokio::fs::create_dir_all(path.parent().unwrap()).await?;
//         let file = File::options()
//             .create(true)
//             .append(true)
//             .open(&path)
//             .await
//             .context("failed to open WAL file")?;
//         Ok(Self { writer: BufWriter::new(file), path })
//     }

//     pub async fn append(&mut self, op: &WalOp) -> Result<()> {
//         let bytes = serde_json::to_vec(op)?;
//         self.writer.write_u32(bytes.len() as u32).await
//             .context("failed to write WAL record length")?;
//         self.writer.write_all(&bytes).await
//             .context("failed to write WAL record payload")?;
//         self.writer.flush().await?;
//         self.writer.get_mut().sync_data().await?;
//         Ok(())
//     }

//     pub async fn replay(path: impl AsRef<Path>) -> Result<Vec<WalOp>> {
//         let mut ops = Vec::new();
//         if !path.as_ref().exists() {
//             return Ok(ops);
//         }

//         let mut file = File::open(path).await
//             .context("failed to open WAL for replay")?;
//         loop {
//             let mut len_buf = [0u8; 4];
//             match file.read_exact(&mut len_buf).await {
//                 Ok(_) => (),
//                 Err(_) => break,
//             }
//             let len = u32::from_le_bytes(len_buf);
//             let mut buf = vec![0u8; len as usize];
//             file.read_exact(&mut buf).await
//                 .context("failed to read WAL record payload")?;
//             let op: WalOp = serde_json::from_slice(&buf)
//                 .context("failed to deserialize WAL record")?;
//             ops.push(op);
//         }
//         Ok(ops)
//     }
// }
