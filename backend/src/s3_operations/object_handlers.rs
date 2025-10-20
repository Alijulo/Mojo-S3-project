use axum::{
    extract::{Path as AxumPath, State},
    response::{Response, IntoResponse},
    body::Body,
    http::{StatusCode, header, HeaderMap},
    extract::Request,
};

use tokio::{fs::{File, self}, io::{AsyncWriteExt}};
use tokio_util::io::ReaderStream;
use futures::stream::StreamExt;
use std::{io::ErrorKind, time::SystemTime,path::PathBuf};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize}; // <-- Added Serde for Query Params
use md5; //ETAG hashing
use hex; // You may need a hex encoder for the MD5 hash
// Import necessary public items from the main module (defined in main.rs)
use crate::{s3_operations::handler_utils, AppError, AppState};

// --- Metadata Structure ---
// Represents the content of the 'image.jpg.metadata.json' file
// #[derive(Serialize, Deserialize, Debug)]
// pub struct ObjectMetadata {
//     // ETag is typically the MD5 hash of the object content for non-multipart uploads
//     #[serde(rename = "ETag")]
//     pub etag: String,
//     // Content-Type from the request headers
//     #[serde(rename = "Content-Type")]
//     pub content_type: String,
// }

// // Helper to get the metadata path
// fn metadata_path(object_path: &PathBuf) -> PathBuf {
//     let mut meta_path = object_path.clone().into_os_string();
//     meta_path.push(".metadata.json");
//     PathBuf::from(meta_path)
// }


// S3 PUT Object Operation (Upload) - Handles object upload with streaming to disk
// --- Modified PUT Object Operation (Upload) ---
pub async fn put_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    req: Request, // Removed 'mut' as it's not needed for reading headers and consuming the request body
) -> Result<StatusCode, AppError> {
    tracing::info!("PUT Request: Bucket='{}', Key='{}'", bucket, key);

    let path = state.object_path(&bucket, &key);
    let metadata_path = handler_utils::metadata_path(&path);
    let content_type = req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    // 1. Ensure the bucket directory exists
    let bucket_dir = path.parent().ok_or_else(|| anyhow::anyhow!("Invalid object path parent"))?;
    fs::create_dir_all(bucket_dir).await.context("Failed to create bucket directory")?;

    // Get the request body
    let body = req.into_body();
    let mut stream = body.into_data_stream();

    // 2. Stream the body chunks to the file on disk (and calculate MD5)
    
    // Create a new MD5 context for ETag calculation (Requires `md-5` or similar crate)
    let mut md5_context = md5::Context::new(); 
    let mut file = File::create(&path).await.context("Failed to create file for PUT")?;

    // Stream the body chunks to the file and update MD5 hash
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Failed to get next chunk from request body")?;
        
        // Update MD5 hash with the current chunk
        md5_context.consume(&chunk);

        // Write chunk to file
        file.write_all(&chunk).await.context("Failed to write chunk to file")?;
    }
    
    // Explicitly flush and close the file to ensure all data is written
    file.flush().await.context("Failed to flush file data")?;
    // File is closed when 'file' goes out of scope, but an explicit sync might be safer in production
    // For tokio::fs::File, there's no direct `sync_all` equivalent exposed for async, 
    // but the `flush` above is a good step.

    // 3. Calculate Final ETag
    // ✅ Corrected line
    let digest_array: [u8; 16] = md5_context.finalize().into(); 
    let etag_digest = format!("\"{}\"", hex::encode(digest_array));
    // 4. Create and Save Metadata JSON file
    let metadata = handler_utils::ObjectMetadata {
        etag: etag_digest.clone(),
        content_type: content_type.clone()
    };

    let metadata_json = serde_json::to_string_pretty(&metadata)
        .context("Failed to serialize metadata to JSON")?;

    fs::write(&metadata_path, metadata_json)
        .await
        .context("Failed to write metadata file")?;
    
    tracing::info!("Object and Metadata saved successfully to: {} | ETag: {}", path.display(), etag_digest);
    Ok(StatusCode::OK) 
}




// S3 GET Object Operation (Download) - Streams file from disk to the client
// --- Modified GET Object Operation (Download) ---
pub async fn get_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
) -> Result<Response, AppError> {
    tracing::info!("GET Request: Bucket='{}', Key='{}'", bucket, key);

    let path = state.object_path(&bucket, &key);
    let metadata_path = handler_utils::metadata_path(&path);

    // 1. Load the main file
    let file = File::open(&path).await.map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            AppError::NotFound(path.display().to_string())
        } else {
            AppError::Io(e)
        }
    }).context("Failed to open file for GET")?;
    let metadata = file.metadata().await.context("Failed to get file metadata")?;
    let content_length = metadata.len();
    
    // // 2. Load the metadata JSON
    // let metadata_json = fs::read_to_string(&metadata_path).await.map_err(|e| {
    //     if e.kind() == ErrorKind::NotFound {
    //         // Handle case where metadata file is missing (error state)
    //         tracing::error!("Metadata file missing for existing object: {}", metadata_path.display());
    //         AppError::Internal(anyhow::anyhow!("Missing metadata file")) 
    //     } else {
    //         AppError::Io(e)
    //     }
    // }).context("Failed to read metadata file")?;
    
    // let object_meta: ObjectMetadata = serde_json::from_str(&metadata_json)
    //     .context("Failed to deserialize metadata JSON")?;

    let object_meta = handler_utils::load_metadata(&metadata_path).await?;

    // 3. Build the response
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, object_meta.content_type.parse().unwrap());
    headers.insert(header::CONTENT_LENGTH, content_length.into());
    // Include the ETag from the metadata file
    headers.insert(header::ETAG, object_meta.etag.parse().unwrap()); 

    tracing::info!("Streaming object from: {} ({} bytes), ETag: {}", path.display(), content_length, object_meta.etag);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .map_err(|e| anyhow::anyhow!("Failed to build response: {}", e))?
        .into_response())
}


// S3 DELETE Object Operation (DELETE) - Deletes an object (file) from disk.
// --- Modified DELETE Object Operation (DELETE) ---
pub async fn delete_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
) -> Result<StatusCode, AppError> {
    tracing::info!("DELETE Request: Bucket='{}', Key='{}'", bucket, key);

    let path = state.object_path(&bucket, &key);
    let metadata_path = handler_utils::metadata_path(&path);

    let main_delete_result = fs::remove_file(&path).await;
    let meta_delete_result = fs::remove_file(&metadata_path).await;

    match (main_delete_result, meta_delete_result) {
        // 1. SUCCESS: If EITHER deletion was Ok, treat it as successful
        (Ok(_), _) | (_, Ok(_)) => {
            tracing::info!("Object and/or metadata deleted successfully: {}", path.display());
            Ok(StatusCode::NO_CONTENT)
        }
        
        // 2. SUCCESS: If BOTH were an Error, but both were ErrorKind::NotFound
        (Err(e1), Err(e2)) if e1.kind() == ErrorKind::NotFound && e2.kind() == ErrorKind::NotFound => {
            tracing::info!("Object not found, returning 204: {}", path.display());
            Ok(StatusCode::NO_CONTENT)
        }
        
        // 3. FATAL ERROR: Catch any remaining error that MUST be a genuine IO error.
        // The error must be from the main file. This covers (Err(IO), Err(_)).
        (Err(e), _) if e.kind() != ErrorKind::NotFound => {
            tracing::error!("Failed to delete object {}: {}", path.display(), e);
            Err(AppError::Io(e))
        }
        
        // 4. FATAL ERROR: Catch remaining error from the metadata file. 
        // This covers the specific remaining case: (Err(NotFound), Err(IO)).
        (_, Err(e)) if e.kind() != ErrorKind::NotFound => {
            tracing::error!("Failed to delete metadata {}: {}", metadata_path.display(), e);
            Err(AppError::Io(e))
        }

        // 5. Catch-all for completeness, though theoretically unreachable
        _ => {
            // This arm should only be hit if there's a logic flaw, 
            // e.g., an unexpected combination of error kinds.
            tracing::error!("Unforeseen error combination during deletion.");
            Err(AppError::Internal(anyhow::anyhow!("Unforeseen deletion error.")))
        }
    }
}



// // S3 HEAD Object Operation - Retrieves object metadata without the content.
// pub async fn head_object(
//     State(state): State<AppState>,
//     AxumPath((bucket, key)): AxumPath<(String, String)>,
// ) -> Result<Response, AppError> {
//     tracing::info!("HEAD Request: Bucket='{}', Key='{}'", bucket, key);

//     let path = state.object_path(&bucket, &key);

//     let metadata = fs::metadata(&path).await.map_err(|e| {
//         if e.kind() == ErrorKind::NotFound {
//             AppError::NotFound(path.display().to_string())
//         } else {
//             AppError::Io(e)
//         }
//     }).context("Failed to get file metadata for HEAD")?;

//     let content_length = metadata.len();
//     let modified_time: SystemTime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);

//     // Convert SystemTime to Utc DateTime and format as HTTP Date header (RFC 7231)
//     let datetime: DateTime<Utc> = modified_time.into();
//     let last_modified_str = datetime.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

//     let mut headers = HeaderMap::new();
    
//     // Essential S3/HTTP headers
//     headers.insert(header::CONTENT_LENGTH, content_length.into());
//     headers.insert(header::LAST_MODIFIED, last_modified_str.parse().unwrap());
//     headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/octet-stream"));
    
//     // ETag mock using Content-Length
//     // In a real S3 server, you'd calculate and return ETag here.
//     // For a mock, a static or simple ETag can be used.
//     // ETag mock using Content-Length (since a full MD5 hash calculation is complex/slow)
//     headers.insert(header::ETAG, format!("\"{}\"", content_length).parse().unwrap());


//     // Manually insert all headers into the response builder
//     let mut response = Response::builder()
//         .status(StatusCode::OK)
//         .body(Body::empty()) // HEAD must have an empty body
//         // Finalize the response builder and convert to a Response object
//         .map_err(|e| anyhow::anyhow!("Failed to build HEAD response: {}", e))? 
//         .into_response();

//     // Insert headers into the Response object itself
//     response.headers_mut().extend(headers.into_iter());

//     tracing::info!("HEAD success for: {} ({} bytes)", path.display(), content_length);

//     Ok(response)
// }



/// S3 HEAD Object Operation - Retrieves object metadata without the content.
pub async fn head_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
) -> Result<Response, AppError> {
    tracing::info!("HEAD Request: Bucket='{}', Key='{}'", bucket, key);

    let path = state.object_path(&bucket, &key);
    let metadata_path = handler_utils::metadata_path(&path);

    // 1. Get filesystem metadata (for Content-Length and Last-Modified)
    let metadata = fs::metadata(&path).await.map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            AppError::NotFound(path.display().to_string())
        } else {
            AppError::Io(e)
        }
    }).context("Failed to get file metadata for HEAD")?;

    let content_length = metadata.len();
    let modified_time: SystemTime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);

    // Convert SystemTime to Utc DateTime and format as HTTP Date header (RFC 7231 format)
    let datetime: DateTime<Utc> = modified_time.into();
    let last_modified_str = datetime.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    // // 2. Read and parse the sidecar metadata JSON file (for ETag and Content-Type)
    // let metadata_json = fs::read_to_string(&metadata_path).await.map_err(|e| {
    //     if e.kind() == ErrorKind::NotFound {
    //         // Treat missing metadata file for an existing object as a Not Found error (or Internal Error, depending on strictness)
    //         tracing::error!("Metadata file missing for existing object: {}", metadata_path.display());
    //         AppError::NotFound(path.display().to_string()) 
    //     } else {
    //         AppError::Io(e)
    //     }
    // }).context("Failed to read metadata file")?;
    
    // let object_meta: handler_utils::ObjectMetadata = serde_json::from_str(&metadata_json)
    //     .context("Failed to deserialize metadata JSON")?;

    let object_meta = handler_utils::load_metadata(&metadata_path).await?;
    // 3. Build Headers
    let mut headers = HeaderMap::new();
    
    // Essential S3/HTTP headers
    headers.insert(header::CONTENT_LENGTH, content_length.into());
    headers.insert(header::LAST_MODIFIED, last_modified_str.parse().unwrap());
    
    // Read ETag and Content-Type from the JSON file
    headers.insert(header::CONTENT_TYPE, object_meta.content_type.parse().unwrap());
    headers.insert(header::ETAG, object_meta.etag.parse().unwrap()); 

    // 4. Build and return the HEAD response
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty()) // HEAD must have an empty body
        .map_err(|e| anyhow::anyhow!("Failed to build HEAD response: {}", e))? 
        .into_response();

    // Insert headers into the Response object itself
    response.headers_mut().extend(headers.into_iter());

    tracing::info!("HEAD success for: {} ({} bytes), ETag: {}", path.display(), content_length, object_meta.etag);

    Ok(response)
}