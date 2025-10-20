use serde::{Deserialize, Serialize}; // <-- Added Serde for Query Params
use std::{io::ErrorKind,path::PathBuf};
use anyhow::Context;
// Import necessary public items from the main module (defined in main.rs)
use crate::{AppError};
use tokio::fs;


// Helper to get the metadata path
pub fn metadata_path(object_path: &PathBuf) -> PathBuf {
    let mut meta_path = object_path.clone().into_os_string();
    meta_path.push(".metadata.json");
    PathBuf::from(meta_path)
}

// --- Metadata Structure ---
// Represents the content of the 'image.jpg.metadata.json' file
#[derive(Serialize, Deserialize, Debug)]
pub struct ObjectMetadata {
    // ETag is typically the MD5 hash of the object content for non-multipart uploads
    #[serde(rename = "ETag")]
    pub etag: String,
    // Content-Type from the request headers
    #[serde(rename = "Content-Type")]
    pub content_type: String,
}


/// Loads and deserializes metadata JSON from the given path.
pub async fn load_metadata(metadata_path: &PathBuf) -> Result<ObjectMetadata, AppError> {
    // 2. Load the metadata JSON
    let metadata_json = fs::read_to_string(&metadata_path).await.map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            // Handle case where metadata file is missing (error state)
            tracing::error!("Metadata file missing for existing object: {}", metadata_path.display());
            AppError::Internal(anyhow::anyhow!("Missing metadata file")) 
        } else {
            AppError::Io(e)
        }
    }).context("Failed to read metadata file")?;
    
    let object_meta: ObjectMetadata = serde_json::from_str(&metadata_json)
        .context("Failed to deserialize metadata JSON")?;
    
    Ok(object_meta)
}