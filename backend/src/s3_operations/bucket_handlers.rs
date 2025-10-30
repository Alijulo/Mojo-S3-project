use axum::{
    extract::{Path as AxumPath, State},
    http::{StatusCode, HeaderMap},
};
use tokio::fs;
use std::{sync::Arc, time::SystemTime, io::ErrorKind};
use anyhow::Context;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::{
    s3_operations::{handler_utils, auth::{check_bucket_permission, AuthenticatedUser, PermissionLevel}},
    AppState, AppError, ListAllMyBucketsResult, Buckets, Owner, BucketInfo, S3_XMLNS, to_xml_string,
};
use sha2::{Digest, Sha256};
use hex;
use handler_utils::S3Headers;

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

/// S3 PUT Bucket Operation (CREATE)
pub async fn put_bucket(
    State(state): State<Arc<AppState>>,
    AxumPath(bucket): AxumPath<String>,
    user: axum::Extension<AuthenticatedUser>,
) -> Result<(StatusCode, HeaderMap), AppError> {
    tracing::info!("PUT Bucket Request: Bucket='{}', User='{}'", bucket, user.0.username);

    // Validate bucket name
    validate_bucket_name(&bucket)?;

    // Check write permission
    if !check_bucket_permission(&state.pool, &user.0, &bucket, PermissionLevel::ReadWrite.as_str())
        .await
        .map_err(|e| AppError::Internal(e))?
    {
        return Err(AppError::AccessDenied);
    }

    let path = state.bucket_path(&bucket);

    match fs::create_dir(&path).await {
        Ok(_) => {
            tracing::info!("Bucket created: {}, User: {}", path.display(), user.0.username);
            let headers = S3Headers::common_headers();
            Ok((StatusCode::OK, headers))
        },
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            tracing::info!("Bucket already exists: {}, User: {}", path.display(), user.0.username);
            let headers = S3Headers::common_headers();
            Ok((StatusCode::OK, headers))
        },
        Err(e) => {
            tracing::error!("Failed to create bucket {}: {}, User: {}", bucket, e, user.0.username);
            Err(AppError::Io(e))
        }
    }
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