use axum::{
    extract::{Path as AxumPath, State,Extension},
    http::{StatusCode, HeaderMap,header},
};

use tokio::fs;
use std::{sync::Arc, time::SystemTime, io::ErrorKind};
use anyhow::Context;
use chrono::{DateTime, Utc};
use anyhow::anyhow;
use crate::{
    s3_operations::{handler_utils, auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel},metadata::{get, set}},
    AppState, AppError, ListAllMyBucketsResult, Buckets, Owner, BucketInfo, S3_XMLNS, to_xml_string,
};
use sha2::{Digest, Sha256};
use hex;
use handler_utils::S3Headers;



/// Bucket metadata that lives in the `.s3meta` file
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct BucketMeta {
    #[serde(default)]
    quota_bytes: u64,          // 0 = unlimited
    #[serde(default)]
    used_bytes: u64,
    #[serde(default)]
    object_count: u64,
    #[serde(default)]
    versioning: bool,
    #[serde(default)]
    created_at: String,        // ISO-8601
    #[serde(default)]
    owner: String,
    #[serde(default)]
    policy: serde_json::Value,
    #[serde(default)]
    encryption: String,        // "none" | "SSE-S3" | …
}

impl BucketMeta {
    fn new(owner: &str) -> Self {
        Self {
            created_at: chrono::Utc::now().to_rfc3339(),
            owner: owner.to_string(),
            encryption: "none".to_string(),
            ..Default::default()
        }
    }
}


// Validates bucket name per AWS S3 naming rules
fn validate_bucket_name(bucket: &str) -> Result<(), AppError> {
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(AppError::Internal(anyhow::anyhow!("Bucket name must be 3-63 characters")));
    }
    if !bucket.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.') {
        return Err(AppError::Internal(anyhow::anyhow!("Bucket name must contain only lowercase letters, numbers, hyphens, or periods")));
    }
    if bucket.starts_with('.') || bucket.ends_with('.') || bucket.starts_with('-') || bucket.ends_with('-') {
        return Err(AppError::Internal(anyhow::anyhow!("Bucket name cannot start or end with a period or hyphen")));
    }
    Ok(())
}

/* --------------------------------------------------------------------- */
/*  Public router helpers                                                */
/* --------------------------------------------------------------------- */
pub async fn put_bucket_no_subpath(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(bucket): axum::extract::Path<String>,
    user: Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    put_bucket(state, bucket, None, user.0).await
}

pub async fn put_bucket_with_subpath(
    State(state): State<Arc<AppState>>,
    axum::extract::Path((bucket, subpath)): axum::extract::Path<(String, String)>,
    user: Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    put_bucket(state, bucket, Some(subpath), user.0).await
}

/* --------------------------------------------------------------------- */
/*  Core implementation – now uses metadata::{get,set}                  */
/* --------------------------------------------------------------------- */
async fn put_bucket(
    state: Arc<AppState>,
    bucket: String,
    subpath: Option<String>,
    user: AuthenticatedUser,
) -> Result<(StatusCode, HeaderMap), AppError> {
    let subpath = subpath.as_deref().unwrap_or("");

    tracing::info!(
        "PUT Bucket: bucket='{}', subpath='{}', user='{}'",
        bucket, subpath, user.username
    );

    validate_bucket_name(&bucket)?;

    if !check_bucket_permission(&state.pool, &user, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    /* --------------------------------------------------------------- */
    /*  1. Determine final filesystem path                         */
    /* --------------------------------------------------------------- */
    let bucket_root = state.bucket_path(&bucket); // e.g. ./s3_data/my-bucket
    let final_path = if subpath.is_empty() {
        bucket_root.clone()
    } else {
        bucket_root.join(subpath)
    };

    /* --------------------------------------------------------------- */
    /*  2. Create the directory (idempotent)                        */
    /* --------------------------------------------------------------- */
    match fs::create_dir_all(&final_path).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
        Err(e) => {
            tracing::error!("IO error while creating path: {}", e);
            return Err(AppError::Io(e));
        }
    }

    /* --------------------------------------------------------------- */
    /*  3. Real bucket creation (no sub-path) → .s3meta + xattr/json  */
    /* --------------------------------------------------------------- */
    let mut headers = S3Headers::common_headers();

    if subpath.is_empty() {
        headers.insert(header::LOCATION, format!("/{bucket}").parse().unwrap());

        let meta_path = bucket_root.join(".s3meta");

        // Create the hidden file if it does not exist
        if fs::metadata(&meta_path).await.is_err() {
            fs::write(&meta_path, b"").await.map_err(AppError::Io)?;
        }

        // ---- read existing metadata (xattr on Unix, .json on Windows) ----
        let path_str = meta_path
            .to_str()
            .ok_or_else(|| AppError::Internal(anyhow!("non-UTF8 path")))?;

        let meta_json = get(path_str, "user.s3.meta")?
            .and_then(|v| String::from_utf8(v).ok())
            .unwrap_or_default();

        // ---- initialise or load BucketMeta ----
        let meta: BucketMeta = if meta_json.is_empty() {
            BucketMeta::new(&user.username)
        } else {
            serde_json::from_str(&meta_json)
                .unwrap_or_else(|_| BucketMeta::new(&user.username))
        };

        // ---- write back (xattr or .json) ----
        let json = serde_json::to_string(&meta).unwrap();
        set(path_str, "user.s3.meta", json.as_bytes())?;
    } else {
        // ---- folder (prefix) creation – fixed empty-object ETag ----
        headers.insert(
            header::ETAG,
            "\"d41d8cd98f00b204e9800998ecf8427e\"".parse().unwrap(),
        );
    }

    Ok((StatusCode::OK, headers))
}



/// S3 LIST Buckets Operation (GET /)
pub async fn get_all_buckets(
    State(state): State<Arc<AppState>>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    tracing::info!("GET / Request: Listing all buckets for User='{}'", user.0.username);

    let mut buckets = Vec::new();
    let mut entries = fs::read_dir(&state.storage_root).await.context("Failed to read storage root")?;

    while let Some(entry) = entries.next_entry().await.context("Failed to read directory entry")? {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                // Check read permission for this bucket
                if check_bucket_permission(&state.pool, &user.0, name, PermissionLevel::ReadOnly.as_str())
                    .await
                    .map_err(|e| AppError::Internal(e))?
                {
                    let metadata = fs::metadata(&path).await.context("Failed to get bucket metadata")?;
                    let creation_time: SystemTime = metadata.created()
                        .unwrap_or_else(|_| metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH));
                    let datetime: DateTime<Utc> = creation_time.into();
                    let creation_date_str = datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

                    buckets.push(BucketInfo {
                        name: name.to_string(),
                        creation_date: creation_date_str,
                    });
                }
            }
        }
    }

    // Sort buckets lexicographically (AWS S3 behavior)
    buckets.sort_by(|a, b| a.name.cmp(&b.name));

    let response_data = ListAllMyBucketsResult {
        xmlns: S3_XMLNS,
        owner: get_owner_info(),
        buckets: Buckets { bucket: buckets },
    };

    let xml_body = to_xml_string(&response_data).context("Failed to serialize bucket list to XML")?;

    let headers = S3Headers::xml_headers();
    tracing::info!("Listed {} buckets for User: {}", response_data.buckets.bucket.len(), user.0.username);
    Ok((headers, xml_body))
}

/// Utility function to get owner information
/// Returns a stable, AWS-compatible 64-character canonical ID
pub fn get_owner_info() -> Owner {
    // Try to read from environment (optional override)
    let id = std::env::var("S3_OWNER_ID").unwrap_or_else(|_| {
        // Stable seed → same ID every run
        let mut hasher = Sha256::new();
        hasher.update(b"mojo-s3-canonical-owner");
        hex::encode(hasher.finalize()) // 64 hex chars
    });

    let display_name = std::env::var("S3_OWNER_DISPLAY_NAME")
        .unwrap_or_else(|_| "mojo-s3-user".to_string());

    Owner { id, display_name }
}

/// S3 DELETE Bucket Operation
pub async fn delete_bucket(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    tracing::info!("DELETE Bucket Request: Bucket='{}', User='{}'", bucket, user.0.username);

    // Check write permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.bucket_path(&bucket);

    // Check if bucket exists
    if !path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    // Check if bucket is empty
    let mut entries = fs::read_dir(&path).await.context("Failed to read bucket directory")?;
    if entries.next_entry().await.context("Failed to read bucket directory entry")?.is_some() {
        tracing::warn!("Bucket '{}' not empty, cannot delete, User: {}", bucket, user.0.username);
        return Err(AppError::BucketNotEmpty);
    }

    // Delete bucket
    fs::remove_dir(&path).await.context("Failed to delete bucket directory")?;
    tracing::info!("Bucket deleted: {}, User: {}", path.display(), user.0.username);

    let headers = S3Headers::common_headers();
    Ok((StatusCode::NO_CONTENT, headers))
}

/// S3 HEAD Bucket Operation
pub async fn head_bucket(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    tracing::info!("HEAD Bucket Request: Bucket='{}', User='{}'", bucket, user.0.username);

    // Check read permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.bucket_path(&bucket);

    match fs::metadata(&path).await {
        Ok(metadata) if metadata.is_dir() => {
            tracing::info!("Bucket found: {}, User: {}", path.display(), user.0.username);
            let headers = S3Headers::common_headers();
            Ok((StatusCode::OK, headers))
        }
        Ok(_) => {
            tracing::warn!("Path exists but is not a directory: {}, User: {}", path.display(), user.0.username);
            Err(AppError::NotFound(bucket))
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            tracing::info!("Bucket not found: {}, User: {}", path.display(), user.0.username);
            Err(AppError::NotFound(bucket))
        }
        Err(e) => {
            tracing::error!("Failed to check bucket {}: {}, User: {}", bucket, e, user.0.username);
            Err(AppError::Io(e))
        }
    }
}

/// S3 GET Bucket Location Operation
pub async fn get_bucket_location(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    tracing::info!("GET /{}?location Request: Retrieving bucket location, User='{}'", bucket, user.0.username);

    // Check read permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.bucket_path(&bucket);

    match fs::metadata(&path).await {
        Ok(metadata) if metadata.is_dir() => {
            let xml_body = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="{}">us-west-2</LocationConstraint>"#,
                S3_XMLNS
            );
            let headers = S3Headers::xml_headers();
            tracing::info!("Location response for '{}': us-west-2, User: {}", bucket, user.0.username);
            Ok((headers, xml_body))
        }
        Ok(_) => Err(AppError::NotFound(bucket)),
        Err(e) if e.kind() == ErrorKind::NotFound => Err(AppError::NotFound(bucket)),
        Err(e) => {
            tracing::error!("Failed to check bucket {}: {}, User: {}", bucket, e, user.0.username);
            Err(AppError::Io(e))
        }
    }
}

/// S3 GET Bucket Versioning Operation
pub async fn get_bucket_versioning(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    tracing::info!("GET /{}?versioning Request: Retrieving bucket versioning status, User='{}'", bucket, user.0.username);

    // Check read permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.bucket_path(&bucket);

    if !path.exists() || !path.is_dir() {
        return Err(AppError::NotFound(bucket));
    }

    let xml_body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="{}">
    <Status>Disabled</Status>
    <MfaDelete>Disabled</MfaDelete>
</VersioningConfiguration>"#,
        S3_XMLNS
    );

    let headers = S3Headers::xml_headers();
    tracing::info!("Versioning response for '{}': Disabled, User: {}", bucket, user.0.username);
    Ok((headers, xml_body))
}

/// S3 GET Bucket Encryption Operation
pub async fn get_bucket_encryption(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    tracing::info!("GET /{}?encryption Request: Retrieving bucket encryption settings, User='{}'", bucket, user.0.username);

    // Check read permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.bucket_path(&bucket);

    if !path.exists() || !path.is_dir() {
        return Err(AppError::NotFound(bucket));
    }

    let xml_body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration xmlns="{}">
    <Rule>
        <ApplyServerSideEncryptionByDefault>
            <SSEAlgorithm>AES256</SSEAlgorithm>
        </ApplyServerSideEncryptionByDefault>
        <BucketKeyEnabled>false</BucketKeyEnabled>
    </Rule>
</ServerSideEncryptionConfiguration>"#,
        S3_XMLNS
    );

    let headers = S3Headers::xml_headers();
    tracing::info!("Encryption response for '{}': AES256, User: {}", bucket, user.0.username);
    Ok((headers, xml_body))
}




//========Get root Storage and no of items
// === 1. Define the XML struct FIRST ===
#[derive(serde::Serialize)]
pub struct RootStorageUsage {
    #[serde(rename = "TotalBuckets")]
    total_buckets: u64,

    #[serde(rename = "TotalObjects")]
    total_objects: u64,

    #[serde(rename = "StorageUsed")]
    storage_used: String,
}

// === 2. Define helper BEFORE the handler ===
fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

//TODO
// CREATE TABLE bucket_usage (
//     bucket_name TEXT PRIMARY KEY,
//     object_count BIGINT NOT NULL DEFAULT 0,
//     total_bytes BIGINT NOT NULL DEFAULT 0
// );


// === 3. Now define the handler ===
/// GET /?usage – XML usage summary
pub async fn get_root_usage(
    State(state): State<Arc<AppState>>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    tracing::info!("GET /?usage (XML) – computing for User='{}'", user.0.username);

    let mut total_buckets = 0u64;
    let mut total_objects = 0u64;
    let mut total_bytes   = 0u64;

    let mut dir = fs::read_dir(&state.storage_root)
        .await
        .with_context(|| "Failed to open storage root")?;

    while let Some(entry) = dir.next_entry()
        .await
        .with_context(|| "Failed to read root entry")?
    {
        let path = entry.path();
        if !path.is_dir() { continue; }

        let bucket = path.file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Invalid bucket name")))?;

        if !check_bucket_permission(&state.pool, &user.0, bucket, PermissionLevel::ReadOnly.as_str())
            .await
            .map_err(|e| AppError::Internal(e))?
        {
            continue;
        }

        total_buckets += 1;

        let mut bucket_dir = fs::read_dir(&path)
            .await
            .with_context(|| format!("Failed to open bucket {bucket}"))?;

        while let Some(obj) = bucket_dir.next_entry()
            .await
            .with_context(|| format!("Failed to read object in {bucket}"))?
        {
            if obj.path().is_file() {
                let meta = obj.metadata()
                    .await
                    .with_context(|| format!("Failed to stat object in {bucket}"))?;
                total_objects += 1;
                total_bytes   += meta.len();
            }
        }
    }

    let usage = RootStorageUsage {
        total_buckets,
        total_objects,
        storage_used: human_bytes(total_bytes),  // used here
    };

    let xml_body = to_xml_string(&usage)
        .with_context(|| "Failed to serialize RootStorageUsage to XML")?;

    let headers = S3Headers::xml_headers();

    tracing::info!(
        "Usage (XML) for '{}': {} buckets, {} objects, {}",
        user.0.username,
        total_buckets,
        total_objects,
        human_bytes(total_bytes)  // used here again
    );

    Ok((headers, xml_body))
}