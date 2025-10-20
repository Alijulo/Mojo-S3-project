use axum::{
    extract::{Path as AxumPath, State,Query},
    http::{StatusCode,HeaderMap},
};

use tokio::fs;
use std::{io::ErrorKind, time::SystemTime,collections::HashSet};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::Deserialize; // <-- Added Serde for Query Params
use std::path::{Path, PathBuf}; // <-- Added PathBuf

// Import necessary public items from the main module (defined in main.rs)
use crate::{s3_operations::handler_utils, AppState, AppError, ListAllMyBucketsResult, Buckets, Owner, BucketInfo, ListBucketResult, ObjectInfo,CommonPrefix, S3_XMLNS, to_xml_string};

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
    // V2 clients might send "list-type", we ignore it for V1 implementation
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
        
        let metadata_result = fs::metadata(&path).await;

        // Skip entries we can't get metadata for (permissions, etc.)
        if let Err(_) = metadata_result { continue; } 
        let metadata = metadata_result.unwrap();

        if metadata.is_file() {
            // 🛑 CRITICAL FIX: Skip metadata files
            if let Some(file_name) = path.file_name().and_then(|name| name.to_str()) {
                if file_name.ends_with(".metadata.json") {
                    continue; // Skip the metadata file
                }
            }
            
            // Calculate the key relative to the bucket root
            if let Ok(relative_path) = path.strip_prefix(base_path) {
                let modified_time = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                let size = metadata.len();
                // Only push the actual object file key
                all_keys.push((relative_path.to_path_buf(), modified_time, size));
            }
        } else if metadata.is_dir() {
            // Recurse into subdirectories
            // FIX: Box::pin() is used to fix the "recursive async fn requires boxing" error.
            Box::pin(traverse_and_get_keys(&path, base_path, all_keys)).await?;
        }
    }
    Ok(())
}



/// S3 PUT Bucket Operation (CREATE) - Creates a new directory for the bucket.
pub async fn put_bucket(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
) -> Result<StatusCode, AppError> {
    tracing::info!("PUT Bucket Request: Bucket='{}'", bucket);

    let path = state.bucket_path(&bucket);

    match fs::create_dir(&path).await {
        Ok(_) => {
            tracing::info!("Bucket created successfully: {}", path.display());
            Ok(StatusCode::OK) 
        },
        Err(e) => {
            if e.kind() == ErrorKind::AlreadyExists {
                tracing::info!("Bucket already exists: {}", path.display());
                Ok(StatusCode::OK) 
            } else {
                tracing::error!("Failed to create bucket {}: {}", bucket, e);
                Err(AppError::Io(e))
            }
        }
    }
}

/// S3 LIST Buckets Operation (GET /) - Lists all directories in the storage root.
pub async fn get_all_buckets(State(state): State<AppState>) -> Result<(HeaderMap, String), AppError> {
    tracing::info!("GET / Request: Listing all buckets (XML).");
    
    let mut buckets = Vec::new();
    
    let mut entries = fs::read_dir(&state.storage_root).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                
                let metadata = fs::metadata(&path).await.context("Failed to get bucket metadata")?;
                
                // Get creation time, falling back to modification time or epoch
                let creation_time: SystemTime = metadata.created()
                    .unwrap_or_else(|_| metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH));
                
                // Convert SystemTime to Utc DateTime and format as ISO 8601 (RFC3339)
                let datetime: DateTime<Utc> = creation_time.into();
                let creation_date_str = datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

                buckets.push(BucketInfo { 
                    name: name.to_string(),
                    creation_date: creation_date_str,
                });
            }
        }
    }

    let response_data = ListAllMyBucketsResult {
        xmlns: S3_XMLNS,
        owner: Owner {
            id: "fake_user_id".to_string(),
            display_name: "fake_user".to_string(),
        },
        buckets: Buckets { bucket: buckets },
    };

    let xml_body = to_xml_string(&response_data).context("Failed to serialize bucket list to XML")?;

    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("application/xml"));

    Ok((headers, xml_body))
}


// /// S3 DELETE Bucket Operation (DELETE) - Deletes an empty directory for the bucket.
// pub async fn delete_bucket(
//     State(state): State<AppState>,
//     AxumPath(bucket): AxumPath<String>,
// ) -> Result<StatusCode, AppError> {
//     tracing::info!("DELETE Bucket Request: Bucket='{}'", bucket);

//     let path = state.bucket_path(&bucket);

//     match fs::remove_dir(&path).await {
//         Ok(_) => {
//             tracing::info!("Bucket deleted successfully: {}", path.display());
//             Ok(StatusCode::NO_CONTENT)
//         }
//         Err(e) => {
//             if e.kind() == ErrorKind::NotFound {
//                 tracing::info!("Bucket not found (already deleted), returning 204: {}", path.display());
//                 Ok(StatusCode::NO_CONTENT)
//             } else {
//                 tracing::error!("Failed to delete bucket {}: {}", path.display(), e);
//                 Err(AppError::Io(e))
//             }
//         }
//     }
// }


// //Working but deleting even when there is contents
// pub async fn delete_bucket(
//     State(state): State<AppState>,
//     axum::extract::Path(bucket): axum::extract::Path<String>,
// ) -> Result<StatusCode, AppError> {
//     tracing::info!("DELETE Bucket Request: Bucket='{}'", bucket);

//     let path = state.bucket_path(&bucket);

//     match fs::remove_dir_all(&path).await {
//         Ok(_) => {
//             tracing::info!("Bucket deleted successfully: {}", path.display());
//             Ok(StatusCode::NO_CONTENT)
//         }
//         Err(e) if e.kind() == ErrorKind::NotFound => {
//             tracing::info!("Bucket not found (already deleted): {}", path.display());
//             Ok(StatusCode::NO_CONTENT)
//         }
//         Err(e) => {
//             tracing::error!("Failed to delete bucket {}: {}", path.display(), e);
//             Err(AppError::Io(e))
//         }
//     }
// }


pub async fn delete_bucket(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
) -> Result<StatusCode, AppError> {
    tracing::info!("DELETE Bucket Request: Bucket='{}'", bucket);

    let path = state.bucket_path(&bucket);

    // Check if bucket exists
    if !path.exists() {
        tracing::info!("Bucket not found (already deleted): {}", path.display());
        return Ok(StatusCode::NO_CONTENT);
    }

    // Check if directory is empty
    let mut entries = match fs::read_dir(&path).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(StatusCode::NO_CONTENT),
        Err(e) => {
            tracing::error!("Failed to read bucket {}: {}", path.display(), e);
            return Err(AppError::Io(e));
        }
    };

    if entries.next_entry().await?.is_some() {
        tracing::warn!("Bucket '{}' not empty, cannot delete", bucket);
        return Ok(StatusCode::CONFLICT); // 409 Conflict
    }

    // Safe to remove
    match fs::remove_dir_all(&path).await {
        Ok(_) => {
            tracing::info!("Bucket deleted successfully: {}", path.display());
            return Ok(StatusCode::NO_CONTENT)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(StatusCode::NO_CONTENT),
        Err(e) => {
            tracing::error!("Failed to delete bucket {}: {}", path.display(), e);
            Err(AppError::Io(e))
        }
    }
}


// S3 HEAD Bucket Operation - Checks if a bucket exists.
pub async fn head_bucket(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
) -> Result<StatusCode, AppError> {
    tracing::info!("HEAD Bucket Request: Bucket='{}'", bucket);

    let path = state.bucket_path(&bucket);

    match fs::metadata(&path).await {
        Ok(metadata) => {
            if metadata.is_dir() {
                // S3 returns 200 OK if the bucket exists and is accessible
                tracing::info!("Bucket found: {}", path.display());
                Ok(StatusCode::OK) 
            } else {
                // If the path exists but isn't a directory (highly unlikely in this setup)
                tracing::warn!("Path exists but is not a directory: {}", path.display());
                Err(AppError::NotFound(bucket))
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // S3 returns 404 Not Found if the bucket does not exist
            tracing::info!("Bucket not found: {}", path.display());
            Err(AppError::NotFound(bucket))
        }
        Err(e) => {
            tracing::error!("Failed to check bucket {}: {}", bucket, e);
            Err(AppError::Io(e))
        }
    }
}


// /// S3 LIST Objects Operation (GET /{bucket}) - Lists all files and their sizes in a bucket.
// pub async fn list_objects(
//     State(state): State<AppState>,
//     AxumPath(bucket): AxumPath<String>,
// ) -> Result<(HeaderMap, String), AppError> {
//     tracing::info!("GET /{} Request: Listing objects (XML).", bucket);

//     let path = state.bucket_path(&bucket);
//     let mut objects = Vec::new();

//     if !path.exists() {
//         return Err(AppError::NotFound(bucket));
//     }

//     let mut entries = fs::read_dir(&path).await.context("Failed to read bucket directory")?;

//     while let Some(entry) = entries.next_entry().await.context("Failed to read directory entry")? {
//         let path = entry.path();
        
//         let metadata = fs::metadata(&path).await.context("Failed to get file metadata")?;

//         if metadata.is_file() {
//             if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                
//                 let modified_time: SystemTime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);

//                 // Convert SystemTime to Utc DateTime and format as ISO 8601 (RFC3339)
//                 let datetime: DateTime<Utc> = modified_time.into();
//                 let last_modified_str = datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

//                 objects.push(ObjectInfo { 
//                     key: name.to_string(),
//                     last_modified: last_modified_str,
//                     size_bytes: metadata.len(),
//                 });
//             }
//         }
//     }

//     let response_data = ListBucketResult {
//         xmlns: S3_XMLNS,
//         bucket_name: bucket.clone(),
//         objects,
//         max_keys: 1000,
//         is_truncated: false,
//     };

//     let xml_body = to_xml_string(&response_data).context("Failed to serialize object list to XML")?;

//     let mut headers = HeaderMap::new();
//     headers.insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("application/xml"));

//     Ok((headers, xml_body))
// }





// S3 LIST Objects Operation (GET /{bucket}) - Lists all files and their sizes in a bucket.
pub async fn list_objects(
    State(state): State<AppState>,
    Query(query): Query<ListObjectsQuery>, // <-- Accepts S3 query parameters
    AxumPath(bucket): AxumPath<String>,
) -> Result<(HeaderMap, String), AppError> {
    
    tracing::info!("GET /{} Request: Listing objects. Query: {:?}", bucket, query);

    let bucket_root_path = state.bucket_path(&bucket);
    let mut objects: Vec<ObjectInfo> = Vec::new();
    // Use a HashSet to automatically deduplicate common prefixes (folders)
    let mut common_prefixes_strings: HashSet<String> = HashSet::new(); 
    let mut all_keys_metadata = Vec::new();

    if !bucket_root_path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    // 1. Get ALL keys recursively (to simulate S3's flat namespace)
    traverse_and_get_keys(&bucket_root_path, &bucket_root_path, &mut all_keys_metadata).await?;
    
    // Sort keys lexicographically, as S3 does
    all_keys_metadata.sort_by(|a, b| a.0.cmp(&b.0));

    let prefix = query.prefix.unwrap_or_default();
    let delimiter = query.delimiter.unwrap_or_default();
    let max_keys = query.max_keys.unwrap_or(1000);
    let marker = query.marker.unwrap_or_default();
    let mut keys_processed = 0;
    
    // 2. Filter, group, and process results
    for (key_path, modified_time, size_bytes) in all_keys_metadata {
        let key = key_path.to_string_lossy().replace("\\", "/"); // Normalize to S3 standard / separator

        // Apply Marker (Start listing AFTER this key)
        if !marker.is_empty() && key <= marker {
            continue;
        }

        // Apply Prefix Filter
        if !prefix.is_empty() && !key.starts_with(&prefix) {
            continue;
        }

        // Apply Max Keys limit (Pagination)
        if keys_processed >= max_keys {
             // If we hit max_keys, we stop and set is_truncated to true below.
             break;
        }

        if !delimiter.is_empty() {
            // Check for delimiter after the prefix
            if let Some(pos) = key[prefix.len()..].find(&delimiter) {
                // Found delimiter: This is a "folder" (CommonPrefix)
                let common_prefix = &key[0..prefix.len() + pos + delimiter.len()];
                common_prefixes_strings.insert(common_prefix.to_string());
                continue; // Skip adding as a full object
            }
        }
        
        // If it passed all filters/delimiter grouping, add as a full object
        
        // Convert SystemTime to Utc DateTime and format as ISO 8601 (RFC3339)
        let datetime: DateTime<Utc> = modified_time.into();
        let last_modified_str = datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        
        // Construct the absolute path for ETag calculation
        let absolute_path = bucket_root_path.join(&key_path);
        //let etag_mock = format!("\"{}\"", size_bytes); // Using size as a mock ETag for listing performance
        //let etag = md5_etag::calculate_md5_etag(&absolute_path).await?;
                // --- FIX: Read ETag from metadata JSON instead of calculating MD5 ---
        let absolute_metadata_path = handler_utils::metadata_path(&absolute_path);

        // // 1. Load the metadata JSON, handling NotFound as an Internal Error
        // let metadata_json = fs::read_to_string(&absolute_metadata_path).await.map_err(|e| {
        //     if e.kind() == ErrorKind::NotFound {
        //         tracing::error!("Metadata file missing for existing object: {}", absolute_metadata_path.display());
        //         // If the data file exists, but metadata is missing, it's an internal consistency error.
        //         AppError::Internal(anyhow::anyhow!("Missing metadata file for object: {}", key))
        //     } else {
        //         AppError::Io(e)
        //     }
        // }).context(format!("Failed to read metadata file for key: {}", key))?;
        
        // // 2. Deserialize the metadata
        // let object_meta: handler_utils::ObjectMetadata = serde_json::from_str(&metadata_json)
        //     .context(format!("Failed to deserialize metadata for key: {}", key))?;
        
        let object_meta = handler_utils::load_metadata(&absolute_metadata_path).await?;
        let etag = object_meta.etag;

        objects.push(ObjectInfo { 
            key: key.to_string(),
            last_modified: last_modified_str,
            size_bytes,
            etag: etag,
        });
        
        keys_processed += 1;
    }

    // 3. Construct the XML response
    let response_data = ListBucketResult {
        xmlns: S3_XMLNS,
        bucket_name: bucket.clone(),
        prefix: if prefix.is_empty() { None } else { Some(prefix) },
        delimiter: if delimiter.is_empty() { None } else { Some(delimiter) },
        marker: if marker.is_empty() { None } else { Some(marker) },
        max_keys,
        is_truncated: keys_processed >= max_keys, // Set true if we stopped listing due to limit
        objects,
        // FIX: Map the collected String prefixes into the required CommonPrefix structs
        common_prefixes: common_prefixes_strings.into_iter().map(|p| CommonPrefix { prefix: p }).collect(),
    };

    let xml_body = to_xml_string(&response_data).context("Failed to serialize object list to XML")?;

    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("application/xml"));

    Ok((headers, xml_body))
}