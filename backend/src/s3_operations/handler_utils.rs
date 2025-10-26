use serde::{Deserialize, Serialize}; // <-- Added Serde for Query Params
use std::{io::ErrorKind,path::PathBuf};
use anyhow::Context;
// Import necessary public items from the main module (defined in main.rs)
use crate::{AppError};
use tokio::fs;
// s3_operations/aws_headers.rs
use axum::http::HeaderMap;
use uuid::Uuid;
use quick_xml::se::to_string;

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

// s3_operations/error_responses.rs
#[derive(Serialize)]
#[serde(rename = "Error")]
pub struct S3Error {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Resource")]
    pub resource: String,
    #[serde(rename = "RequestId")]
    pub request_id: String,
    #[serde(rename = "HostId")]
    pub host_id: String,
}


pub struct S3Headers;

impl S3Headers {
    pub fn common_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-request-id", Uuid::new_v4().to_string().parse().unwrap());
        headers.insert("x-amz-id-2", Self::generate_amz_id_2());
        headers.insert("server", "AmazonS3".parse().unwrap());
        headers.insert("date", chrono::Utc::now().to_rfc2822().parse().unwrap());
        headers
    }

    pub fn xml_headers() -> HeaderMap {
        let mut headers = Self::common_headers();
        headers.insert("content-type", "application/xml".parse().unwrap());
        headers
    }

    pub fn object_headers(etag: &str, content_type: &str, content_length: u64) -> HeaderMap {
        let mut headers = Self::common_headers();
        headers.insert("etag", etag.parse().unwrap());
        headers.insert("content-type", content_type.parse().unwrap());
        headers.insert("content-length", content_length.into());
        headers.insert("last-modified", chrono::Utc::now().to_rfc2822().parse().unwrap());
        headers
    }

    pub fn generate_amz_id_2() -> axum::http::HeaderValue {
        // Generate a realistic AWS x-amz-id-2 format
        let id = format!("{}/{}", 
            base64::encode(Uuid::new_v4().as_bytes()),
            base64::encode(Uuid::new_v4().as_bytes())
        );
        id.parse().unwrap()
    }
}

impl S3Error {
    pub fn new(code: &str, message: &str, resource: &str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            resource: resource.to_string(),
            request_id: Uuid::new_v4().to_string(),
            host_id: S3Headers::generate_amz_id_2().to_str().unwrap().to_string(),
        }
    }

    pub fn to_xml(&self) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>{}"#,
            to_string(self).unwrap_or_else(|_| "Serialization error".to_string())
        )
    }

    pub fn not_found(resource: &str) -> Self {
        Self::new("NoSuchBucket", "The specified bucket does not exist", resource)
    }

    pub fn access_denied(resource: &str) -> Self {
        Self::new("AccessDenied", "Access Denied", resource)
    }

    pub fn bucket_not_empty(resource: &str) -> Self {
        Self::new("BucketNotEmpty", "The bucket you tried to delete is not empty", resource)
    }

    pub fn no_such_key(resource: &str) -> Self {
        Self::new("NoSuchKey", "The specified key does not exist", resource)
    }
}


