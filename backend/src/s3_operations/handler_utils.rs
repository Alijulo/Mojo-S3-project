use serde::{Deserialize, Serialize}; // <-- Added Serde for Query Params
use std::{io::ErrorKind,path::PathBuf};
use anyhow::Context;
// Import necessary public items from the main module (defined in main.rs)
use thiserror::Error;
use tokio::fs;
// s3_operations/aws_headers.rs
use axum::http::{HeaderMap,header};
use uuid::Uuid;
use quick_xml::se::to_string;
use base64::{Engine as _, engine::general_purpose};
use axum::{
    response::{IntoResponse, Response},
    http::StatusCode,

};
use tracing::{error, info, Level};



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
        headers.insert("connection", "close".parse().unwrap());
        headers
    }

    pub fn generate_amz_id_2() -> axum::http::HeaderValue {
        // Generate a realistic AWS x-amz-id-2 format
        let id = format!("{}/{}", 
            general_purpose::STANDARD.encode(Uuid::new_v4().as_bytes()),
            general_purpose::STANDARD.encode(Uuid::new_v4().as_bytes())
        );
        id.parse().unwrap()
    }
    pub fn json_headers() -> HeaderMap {
        let mut headers = Self::common_headers();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        headers
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


// --- Authentication Request Structure (JSON input) ---
#[derive(Deserialize)]
struct AuthRequest {
    username: String,
    password: String,
}

// --- XML Authentication Response Structures ---
#[derive(Serialize)]
#[serde(rename = "AuthResponse")]
pub struct AuthResponseXml {
    #[serde(rename = "Token")]
    pub token: String,
    #[serde(rename = "Username")]
    pub username: String,
    #[serde(rename = "Role")]
    pub role: String,
    #[serde(rename = "UserId")]
    pub user_id: i64,
}



#[derive(Serialize)]
pub struct Owner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}


// --- List Objects (GET /{bucket}) ---
#[derive(Debug, Serialize)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

#[derive(Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListBucketResult {
    #[serde(rename = "@xmlns")]
    pub xmlns: &'static str,
    #[serde(rename = "Name")]
    pub bucket_name: String,
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(rename = "Marker", skip_serializing_if = "Option::is_none")]
    pub marker: Option<String>,
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "CommonPrefixes", skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
    #[serde(rename = "Contents")]
    pub objects: Vec<ObjectInfo>,
}

#[derive(Debug, Serialize)]
pub struct ObjectInfo {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size_bytes: u64,
}

// --- S3-Oriented Custom Error Handling ---
#[derive(Debug, Error)]
pub enum AppError {
    #[error("Storage I/O Error: {0}")]
    Io(#[from] tokio::io::Error),
    #[error("Not Found: {0}")]
    NotFound(String),
    #[error("Internal Server Error: {0}")]
    Internal(#[source] anyhow::Error),
    #[error("Access denied: unauthorized or invalid credentials.")]
    AccessDenied,
    #[error("Bucket not empty")]
    BucketNotEmpty,
    #[error("No such key")]
    NoSuchKey,
    #[error("No such upload")]
    NoSuchUpload,
    #[error("No such version")]
    NoSuchVersion,
    #[error("Invalid bucket name: {0}")]
    InvalidBucketName(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Invalid range: {0}")]
    InvalidRange(String),
    #[error("Precondition failed")]
    PreconditionFailed,
    #[error("Entity too large")]
    EntityTooLarge,
    #[error("Bad request: {0}")]
    BadRequest(String),
    /// S3: Content-MD5 mismatch
    #[error("BadDigest: {0}")]
    BadDigest(String),
    /// S3: XAmzContentSHA256Mismatch
    #[error("XAmzContentSHA256Mismatch")]
    Sha256Mismatch,
    
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("Application Error: {:?}", self);

        let (status, s3_error) = match self {
            AppError::Io(e) => {
                match e.kind() {
                    ErrorKind::NotFound => (
                        StatusCode::NOT_FOUND,
                        S3Error::not_found("resource")
                    ),
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        S3Error::new("InternalError", &format!("Internal error: {}", e), "resource")
                    ),
                }
            },
            AppError::NotFound(resource) => (
                StatusCode::NOT_FOUND,
                S3Error::not_found(&resource)
            ),
            AppError::BucketNotEmpty => (
                StatusCode::CONFLICT,
                S3Error::bucket_not_empty("bucket")
            ),
            AppError::NoSuchKey => (
                StatusCode::NOT_FOUND,
                S3Error::no_such_key("key")
            ),
            AppError::NoSuchUpload => ( 
                StatusCode::NOT_FOUND,
                S3Error::new("NoSuchUpload", "The specified upload does not exist.", "upload")
            ),
            AppError::NoSuchVersion => (
                StatusCode::NOT_FOUND,
                S3Error::new(
                    "NoSuchVersion",
                    "The specified version does not exist.",
                    "versionId"
                )
            ),
            AppError::AccessDenied => (
                StatusCode::FORBIDDEN,
                S3Error::access_denied("resource")
            ),
            AppError::InvalidBucketName(msg) => (
                StatusCode::BAD_REQUEST,
                S3Error::new("InvalidBucketName", &msg, "bucket"),
            ),
            AppError::InvalidArgument(msg) => (
                StatusCode::BAD_REQUEST,
                S3Error::new("InvalidArgument", &msg, "argument"),
            ),
            AppError::InvalidRange(msg) => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                S3Error::new("InvalidRange", &msg, "range"),
            ),

            AppError::PreconditionFailed => (
                StatusCode::PRECONDITION_FAILED,
                S3Error::new("PreconditionFailed", "Precondition failed", "resource"),
            ),
            AppError::EntityTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                S3Error::new("EntityTooLarge", "The object is too large", "resource"),
            ),
            AppError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                S3Error::new("BadRequest", &msg, "request")
            ),
            AppError::BadDigest(msg) => (
                StatusCode::BAD_REQUEST,
                S3Error::new("BadDigest", &msg, "object"),
            ),
            AppError::Sha256Mismatch => (
                StatusCode::BAD_REQUEST,
                S3Error::new("XAmzContentSHA256Mismatch", "The SHA256 checksum did not match", "object"),
            ),
            AppError::Internal(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                S3Error::new("InternalError", &format!("Internal error: {}", e), "resource")
            ),
        };

        let headers = S3Headers::xml_headers();
        let body = s3_error.to_xml();
        
        (status, headers, body).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(error: anyhow::Error) -> Self {
        AppError::Internal(error)
    }
}

