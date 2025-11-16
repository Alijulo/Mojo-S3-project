use axum::{
    extract::{Path as AxumPath, State,Extension},
    http::{StatusCode, HeaderMap,header,},
};

use tokio::fs;
use std::{sync::Arc, io::ErrorKind};
use anyhow::Context;
use chrono::{DateTime, Utc};
use anyhow::anyhow;
use crate::{
    s3_operations::{handler_utils, auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel},metadata::{get, set}},
    AppState, S3_XMLNS, to_xml_string,GLOBAL_IO_SEMAPHORE,
};
use hex;
use handler_utils::{S3Headers,AppError, Owner,};
use tracing::{error, info};
use serde::{Serialize, Deserialize,};
use serde_json::Value;
use http::header::InvalidHeaderValue;


// ====================================================================
// Bucket Metadata
// ====================================================================
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct BucketMeta {
    #[serde(default)]
    pub quota_bytes: u64, // 0 = no quota
    #[serde(default)]
    pub used_bytes: u64,
    #[serde(default)]
    pub object_count: u64,
    #[serde(default)]
    pub versioning: bool,
    #[serde(default)]
    pub created_at: String, // ISO-8601 RFC3339
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub policy: Value,
    #[serde(default)]
    pub encryption: String, // "none" | "SSE-S3" | "SSE-KMS"
}


// ====================================================================
// 2. BucketMetadataResponse – only for XML API response
// ====================================================================
#[derive(Serialize)]
#[serde(rename = "BucketMetadata")]
struct BucketMetadataResponse {
    #[serde(rename = "QuotaBytes")]
    quota_bytes: u64,

    #[serde(rename = "UsedBytes")]
    used_bytes: u64,

    #[serde(rename = "ObjectCount")]
    object_count: u64,

    #[serde(rename = "Versioning")]
    versioning: bool,

    #[serde(rename = "CreatedAt")]
    created_at: String,

    #[serde(rename = "Owner")]
    owner: String,

    #[serde(rename = "Policy")]
    policy: Value,

    #[serde(rename = "Encryption")]
    encryption: String,

    // Only in response – computed at runtime
    #[serde(rename = "Permission")]
    permission: String,
}

impl BucketMeta {
    fn new(owner: &str) -> Self {
        Self {
            created_at: chrono::Utc::now().to_rfc3339(),
            owner: owner.to_string(),
            encryption: "none".to_string(),
            policy: serde_json::json!({}),
            ..Default::default()
        }
    }
}


#[derive(Serialize)]
pub struct BucketInfo {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "CreationDate")]
    pub creation_date: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct ListAllMyBucketsResult {
    #[serde(rename = "xmlns")]
    pub xmlns: String,
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "Buckets")]
    pub buckets: Buckets,
}

#[derive(Serialize)]
pub struct Buckets {
    #[serde(rename = "Bucket")]
    pub bucket: Vec<BucketInfo>,
}

// ====================================================================
// Validation
// ====================================================================
fn validate_bucket_name(bucket: &str) -> Result<(), AppError> {
    let len = bucket.len();
    if !(3..=63).contains(&len) {
        return Err(AppError::InvalidBucketName(format!(
            "Bucket name must be between 3 and 63 characters, got {len}"
        )));
    }

    if !bucket
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
    {
        return Err(AppError::InvalidBucketName(
            "Only lowercase letters, numbers, hyphens, and periods allowed".into(),
        ));
    }

    if bucket.starts_with('.') || bucket.ends_with('.') || bucket.starts_with('-') || bucket.ends_with('-') {
        return Err(AppError::InvalidBucketName(
            "Cannot start or end with '.' or '-'".into(),
        ));
    }

    // Optional: DNS compliance (no IP addresses)
    if bucket
        .split('.')
        .all(|seg| seg.parse::<u8>().is_ok() && seg.len() <= 3)
    {
        return Err(AppError::InvalidBucketName(
            "Bucket name cannot be formatted as an IP address".into(),
        ));
    }

    Ok(())
}

// ====================================================================
// Router Helpers
// ====================================================================
pub async fn put_bucket_no_subpath(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    put_bucket_inner(state, bucket, None, user).await
}

pub async fn put_bucket_with_subpath(
    State(state): State<Arc<AppState>>,
    AxumPath((bucket, subpath)): AxumPath<(String, String)>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    put_bucket_inner(state, bucket, Some(subpath), user).await
}

// ====================================================================
// Core PUT Bucket Logic (idempotent, S3-compatible)
// ====================================================================
async fn put_bucket_inner(
    state: Arc<AppState>,
    bucket: String,
    subpath: Option<String>,
    user: AuthenticatedUser,
) -> Result<(StatusCode, HeaderMap), AppError> {
    let _io_permit = GLOBAL_IO_SEMAPHORE.acquire().await.unwrap(); // throttle IO
    let subpath = subpath.as_deref().unwrap_or("");
    info!("PUT Bucket: bucket='{bucket}', subpath='{subpath}', user='{}'", user.username);
    validate_bucket_name(&bucket)?;
    // Permission check
    let has_perm = check_bucket_permission(&state.pool, &user, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(AppError::Internal)?;
    if !has_perm {
        return Err(AppError::AccessDenied);
    }
    let bucket_root = state.bucket_path(&bucket);
    let final_path = if subpath.is_empty() {
        bucket_root.clone()
    } else {
        bucket_root.join(subpath)
    };
    // Create directory (idempotent)
    if let Err(e) = fs::create_dir_all(&final_path).await {
        if e.kind() != ErrorKind::AlreadyExists {
            error!("Failed to create directory {}: {}", final_path.display(), e);
            return Err(AppError::Io(e));
        }
    }
    let mut headers = S3Headers::common_headers();
    // ----------------------------------------------------------------
    // Case 1: Real bucket creation (no subpath)
    // ----------------------------------------------------------------
    if subpath.is_empty() {
        headers.insert(
            header::LOCATION,
            format!("/{bucket}").parse().map_err(|e: InvalidHeaderValue| AppError::Internal(anyhow!(e)))?,
        );
        let meta_path = bucket_root.join(".s3meta");
        let index_dir = bucket_root.join(".s3index");
        // Ensure marker files exist on Unix
        #[cfg(unix)]
        {
            if fs::metadata(&meta_path).await.is_err() {
                let _ = fs::File::create(&meta_path).await;
            }
        }
        fs::create_dir_all(&index_dir).await.map_err(AppError::Io)?;
        let path_str = meta_path
            .to_str()
            .ok_or_else(|| AppError::Internal(anyhow!("Metadata path is not UTF-8")))?;
        // Read existing metadata
        let existing_json = get(path_str, "user.s3.meta")
            .await
            .ok()
            .flatten()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .unwrap_or_default();
        // Load or initialize metadata
        let mut meta: BucketMeta = if existing_json.is_empty() {
            BucketMeta::new(&user.username)
        } else {
            match serde_json::from_str(&existing_json) {
                Ok(m) => m,
                Err(e) => {
                    error!("Corrupted bucket metadata for {bucket}, resetting: {e}");
                    BucketMeta::new(&user.username)
                }
            }
        };
        // Ensure owner is correct
        if meta.owner.is_empty() {
            meta.owner = user.username.clone();
        }
        // Initialize stats if missing
        meta.object_count = meta.object_count;
        meta.used_bytes = meta.used_bytes;
        meta.versioning = meta.versioning;
        // Serialize and write back atomically
        let json = serde_json::to_string_pretty(&meta).map_err(|e| AppError::Internal(anyhow!(e)))?;
        set(path_str, "user.s3.meta", json.as_bytes()).await?;
        // Add industrial headers
        headers.insert("x-amz-request-id", format!("put-bucket-{bucket}").parse().map_err(|e: InvalidHeaderValue| AppError::Internal(anyhow!(e)))?);
        headers.insert("server", "AmazonS3".parse().map_err(|e: InvalidHeaderValue| AppError::Internal(anyhow!(e)))?);
        return Ok((StatusCode::OK, headers));
    }
    // ----------------------------------------------------------------
    // Case 2: Prefix (folder) creation → return empty object ETag
    // ----------------------------------------------------------------
    headers.insert(
        header::ETAG,
        "\"d41d8cd98f00b204e9800998ecf8427e\"".parse().map_err(|e: InvalidHeaderValue| AppError::Internal(anyhow!(e)))?,
    );
    headers.insert("x-amz-request-id", format!("put-prefix-{bucket}/{subpath}").parse().map_err(|e: InvalidHeaderValue| AppError::Internal(anyhow!(e)))?);
    headers.insert("server", "AmazonS3".parse().map_err(|e: InvalidHeaderValue| AppError::Internal(anyhow!(e)))?);
    Ok((StatusCode::OK, headers))
}



/// S3 List All My Buckets (GET /)
/// Returns <ListAllMyBucketsResult> with bucket names and creation dates
pub async fn get_all_buckets(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    let _io_permit = GLOBAL_IO_SEMAPHORE.acquire().await.unwrap(); // throttle IO
    tracing::info!("GET / – Listing all buckets for user '{}'", user.username);

    let mut buckets = Vec::new();
    let mut entries = fs::read_dir(&state.storage_root)
        .await
        .map_err(AppError::Io)?;

    while let Some(entry) = entries.next_entry().await.map_err(AppError::Io)? {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let bucket_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) if !name.starts_with('.') => name,
            _ => continue,
        };

        // Permission check
        if !check_bucket_permission(&state.pool, &user, bucket_name, PermissionLevel::ReadOnly.as_str())
            .await
            .map_err(AppError::Internal)?
        {
            continue;
        }

        // Load bucket metadata
        let meta_path = path.join(".s3meta");
        let path_str = match meta_path.to_str() {
            Some(s) => s,
            None => {
                tracing::warn!("Skipping bucket with non-UTF8 path: {:?}", path);
                continue;
            }
        };

        let meta_json = match get(path_str, "user.s3.meta").await {
            Ok(Some(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
            Ok(None) => {
                tracing::debug!("No .s3meta for bucket: {}", bucket_name);
                continue;
            }
            Err(e) => {
                tracing::warn!("Failed to read .s3meta for '{}': {}", bucket_name, e);
                continue;
            }
        };

        let meta: BucketMeta = match serde_json::from_str(&meta_json) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("Corrupted .s3meta for bucket '{}': {}", bucket_name, e);
                continue;
            }
        };

        let creation_date = match DateTime::parse_from_rfc3339(&meta.created_at) {
            Ok(dt) => dt.with_timezone(&Utc),
            Err(e) => {
                tracing::warn!("Invalid created_at in .s3meta for '{}': {}", bucket_name, e);
                continue;
            }
        };

        buckets.push(BucketInfo {
            name: bucket_name.to_string(),
            creation_date,
        });
    }

    // Sort lexicographically (AWS S3 behavior)
    buckets.sort_by(|a, b| a.name.cmp(&b.name));

    let owner = get_owner_info(); // ensures correct <Owner> element
    let response = ListAllMyBucketsResult {
        xmlns: S3_XMLNS.to_string(),
        owner,
        buckets: Buckets { bucket: buckets },
    };

    let xml_body = to_xml_string(&response)
        .map_err(|e| AppError::Internal(anyhow!("Failed to serialize XML: {e}")))?;

    let mut headers = S3Headers::xml_headers();
    headers.insert(header::CONTENT_LENGTH, xml_body.len().to_string().parse().unwrap());
    headers.insert("x-amz-request-id", format!("list-buckets-{}", user.username).parse().unwrap());
    headers.insert("server", "AmazonS3".parse().unwrap());

    tracing::info!(
        "Listed {} bucket(s) for user '{}'",
        response.buckets.bucket.len(),
        user.username
    );

    Ok((headers, xml_body))
}


/// Returns a stable, AWS-compatible 64-character canonical ID
/// Returns a stable, AWS-compatible 64-character canonical ID
pub fn get_owner_info() -> Owner {
    use sha2::{Digest, Sha256};
    use std::env;

    // Canonical ID: either from env or derived deterministically
    let id = match env::var("S3_OWNER_ID") {
        Ok(val) if val.len() == 64 && val.chars().all(|c| c.is_ascii_hexdigit()) => val,
        _ => {
            let mut hasher = Sha256::new();
            hasher.update(b"mojo-s3-canonical-owner");
            hex::encode(hasher.finalize()) // always 64 hex chars
        }
    };

    let display_name = env::var("S3_OWNER_DISPLAY_NAME")
        .unwrap_or_else(|_| "mojo-s3-user".to_string());

    Owner { id, display_name }
}




// ====================================================================
// 3. Handler – GET /buckets/{name}/metadata (XML)
// ====================================================================
pub async fn get_bucket_metadata(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    Extension(user): Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    let _io_permit = GLOBAL_IO_SEMAPHORE.acquire().await.unwrap();

    // 1. Compute permission at runtime
    let permission = if check_bucket_permission(&state.pool, &user, &bucket, PermissionLevel::ReadWrite.as_str()).await? {
        "ReadWrite"
    } else if check_bucket_permission(&state.pool, &user, &bucket, PermissionLevel::ReadOnly.as_str()).await? {
        "ReadOnly"
    } else {
        return Err(AppError::AccessDenied);
    };

    // 2. Load .s3meta from disk
    let bucket_path = state.bucket_path(&bucket);
    if !bucket_path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    let meta_path = bucket_path.join(".s3meta");
    let path_str = meta_path
        .to_str()
        .ok_or_else(|| AppError::Internal(anyhow!("Non-UTF8 path")))?;

    let meta_json = get(path_str, "user.s3.meta")
        .await
        .map_err(|e| AppError::Internal(anyhow::Error::from(e)))?
        .ok_or_else(|| AppError::NotFound("metadata".to_string()))?;

    let meta: BucketMeta = serde_json::from_slice(&meta_json)
        .map_err(|e| AppError::Internal(anyhow!("Corrupted .s3meta: {e}")))?;

    // 3. Build response with computed permission + canonical owner
    let owner_info = get_owner_info();
    let response = BucketMetadataResponse {
        quota_bytes: meta.quota_bytes,
        used_bytes: meta.used_bytes,
        object_count: meta.object_count,
        versioning: meta.versioning,
        created_at: meta.created_at,
        owner: owner_info.display_name, // canonical display name
        policy: meta.policy,
        encryption: meta.encryption,
        permission: permission.to_string(),
    };

    // 4. Serialize to XML
    let xml_body = quick_xml::se::to_string_with_root("BucketMetadata", &response)
        .map_err(|e| AppError::Internal(anyhow!("XML serialization failed: {e}")))?;

    // 5. Headers
    let mut headers = S3Headers::xml_headers();
    headers.insert(header::CONTENT_LENGTH, xml_body.len().to_string().parse().unwrap());
    headers.insert("x-amz-request-id", format!("get-bucket-meta-{bucket}").parse().unwrap());
    headers.insert("server", "AmazonS3".parse().unwrap());

    Ok((headers, xml_body))
}


/// S3 DELETE Bucket Operation
pub async fn delete_bucket(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    let _io_permit = GLOBAL_IO_SEMAPHORE.acquire().await.unwrap();
    tracing::info!("DELETE Bucket Request: Bucket='{}', User='{}'", bucket, user.0.username);

    // 1. Permission check
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    // 2. Bucket existence
    let path = state.bucket_path(&bucket);
    if !path.exists() {
        return Err(AppError::NotFound(bucket));
    }

    // 3. Check emptiness (skip system files and ignore empty subfolders)
    let mut entries = fs::read_dir(&path).await.context("Failed to read bucket directory")?;
    while let Some(entry) = entries.next_entry().await.context("Failed to read bucket entry")? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Skip internal markers
        if name_str.starts_with(".s3meta")
            || name_str.starts_with(".s3index")
            || name_str.starts_with(".s3versions")
        {
            continue;
        }

        let entry_path = entry.path();
        if entry_path.is_dir() {
            // Check if subdir contains any user files
            let mut sub_entries = fs::read_dir(&entry_path).await?;
            let mut has_user_files = false;
            while let Some(sub_entry) = sub_entries.next_entry().await? {
                let sub_name = sub_entry.file_name().to_string_lossy().to_string();
                if !sub_name.starts_with('.') {
                    has_user_files = true;
                    break;
                }
            }
            if has_user_files {
                tracing::warn!(
                    "Bucket '{}' not empty (found user files in subdir '{}')",
                    bucket,
                    name_str
                );
                return Err(AppError::BucketNotEmpty);
            }
            // else: empty subfolder → skip
        } else {
            // Any non-hidden file means bucket is not empty
            tracing::warn!(
                "Bucket '{}' not empty (found file '{}')",
                bucket,
                name_str
            );
            return Err(AppError::BucketNotEmpty);
        }
    }

    // 4. Delete bucket (including hidden metadata dirs)
    fs::remove_dir_all(&path).await.context("Failed to delete bucket directory")?;
    tracing::info!("Bucket deleted: {}, User={}", path.display(), user.0.username);

    // 5. Headers
    let mut headers = S3Headers::common_headers();
    headers.insert("x-amz-request-id", format!("delete-bucket-{bucket}").parse().unwrap());
    headers.insert("server", "AmazonS3".parse().unwrap());

    Ok((StatusCode::NO_CONTENT, headers))
}



/// S3 HEAD Bucket Operation
pub async fn head_bucket(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    let _io_permit = GLOBAL_IO_SEMAPHORE.acquire().await.unwrap();
    tracing::info!("HEAD Bucket Request: Bucket='{}', User='{}'", bucket, user.0.username);

    // 1. Permission check
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    // 2. Bucket existence
    let path = state.bucket_path(&bucket);
    if !path.exists() {
        tracing::info!("Bucket not found: {}, User={}", path.display(), user.0.username);
        return Err(AppError::NotFound(bucket));
    }

    // 3. Load bucket metadata
    let meta_path = path.join(".s3meta");
    let path_str = meta_path
        .to_str()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Non-UTF8 path")))?;

    let meta_json = get(path_str, "user.s3.meta")
        .await
        .map_err(|e| AppError::Internal(anyhow::Error::from(e)))?
        .ok_or_else(|| AppError::NotFound("metadata".to_string()))?;

    let meta: BucketMeta = serde_json::from_slice(&meta_json)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Corrupted .s3meta: {e}")))?;

    tracing::info!("Bucket found: {}, Owner={}, User={}", path.display(), meta.owner, user.0.username);

    // 4. Headers
    let mut headers = S3Headers::common_headers();
    headers.insert("x-amz-request-id", format!("head-bucket-{bucket}").parse().unwrap());
    headers.insert("server", "AmazonS3".parse().unwrap());

    Ok((StatusCode::OK, headers))
}

//Put bucket policy
pub async fn put_bucket_policy(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: Extension<AuthenticatedUser>,
    body: String, // raw JSON policy
) -> Result<(StatusCode, HeaderMap), AppError> {
    // Permission check
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    // Validate JSON
    let policy: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| AppError::InvalidArgument(format!("Invalid policy JSON: {e}")))?;

    if !policy.get("Statement").is_some() {
        return Err(AppError::InvalidArgument("Missing Statement".into()));
    }

    // Save to .s3meta
    let meta_path = state.bucket_path(&bucket).join(".s3meta");
    let path_str = meta_path.to_str().ok_or_else(|| AppError::Internal(anyhow!("Non-UTF8 path")))?;
    set(path_str, "user.s3.meta", body.as_bytes()).await?;

    let mut headers = S3Headers::common_headers();
    headers.insert("x-amz-request-id", format!("put-bucket-policy-{bucket}").parse().unwrap());
    headers.insert("server", "AmazonS3".parse().unwrap());

    Ok((StatusCode::OK, headers))
}

//Get Bucket policy
pub async fn get_bucket_policy(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: Extension<AuthenticatedUser>,
) -> Result<(HeaderMap, String), AppError> {
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadOnly.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    let meta_path = state.bucket_path(&bucket).join(".s3meta");
    let path_str = meta_path.to_str().ok_or_else(|| AppError::Internal(anyhow!("Non-UTF8 path")))?;

    let policy_json = get(path_str, "user.s3.policy")
        .await
        .map_err(|e| AppError::Internal(anyhow::Error::from(e)))?
        .ok_or_else(|| AppError::NotFound("policy".to_string()))?;

    let policy_str = String::from_utf8(policy_json).map_err(|e| AppError::Internal(anyhow!(e)))?;

    let mut headers = S3Headers::json_headers();  // Assuming you add this method to S3Headers
    headers.insert("x-amz-request-id", format!("get-bucket-policy-{bucket}").parse().unwrap());
    headers.insert("server", "AmazonS3".parse().unwrap());

    Ok((headers, policy_str))
}

//Deelete bucket
pub async fn delete_bucket_policy(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(AppError::Internal)?
    {
        return Err(AppError::AccessDenied);
    }

    let meta_path = state.bucket_path(&bucket).join(".s3meta");
    let path_str = meta_path.to_str().ok_or_else(|| AppError::Internal(anyhow!("Non-UTF8 path")))?;

    set(path_str, "user.s3.policy", b"{}").await?;

    let mut headers = S3Headers::common_headers();
    headers.insert("x-amz-request-id", format!("delete-bucket-policy-{bucket}").parse().unwrap());
    headers.insert("server", "AmazonS3".parse().unwrap());

    Ok((StatusCode::NO_CONTENT, headers))
}


//Bucket Evalluation and policy validation
pub fn evaluate_bucket_policy(
    policy: &Value,
    action: &str,
    resource: &str,
    principal: &str,
) -> bool {
    if let Some(statements) = policy.get("Statement").and_then(|s| s.as_array()) {
        for stmt in statements {
            let effect = stmt.get("Effect").and_then(|e| e.as_str()).unwrap_or("Deny");

            let actions: &[Value] = stmt
                .get("Action")
                .and_then(|a| a.as_array())
                .map_or(&[], |v| v.as_slice());

            let resources: &[Value] = stmt
                .get("Resource")
                .and_then(|r| r.as_array())
                .map_or(&[], |v| v.as_slice());

            let principals = stmt.get("Principal");

            let action_match = actions.iter().any(|a| a.as_str() == Some(action));
            let resource_match = resources.iter().any(|r| r.as_str() == Some(resource));
            let principal_match = principals.map_or(true, |p| {
                p == "*" || p.as_str() == Some(principal)
            });

            if action_match && resource_match && principal_match {
                return effect == "Allow";
            }
        }
    }
    false // default deny
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