use axum::{
    http::{Request, StatusCode, HeaderMap},
    middleware::{Next},
    response::{IntoResponse, Response},
    Extension,
      extract::{State},
};
use std::sync::Arc;
use base64::prelude::*;
use sqlx::SqlitePool;
use bcrypt::verify;
use dashmap::DashMap;
use lazy_static::lazy_static;
use chrono::{DateTime, Utc, Duration};
use anyhow::{Context, Result};
use crate::s3_operations::user_models::{verify_credentials, User};
use crate::s3_operations::jwt_utils::validate_jwt;
use crate::{AppError, AppState};

// Constants for authentication prefixes
const BEARER_PREFIX: &str = "Bearer ";
const BASIC_PREFIX: &str = "Basic ";
const AWS_SIGNATURE_PREFIX: &str = "AWS4-HMAC-SHA256";

// Enum for permission levels
#[derive(Debug, PartialEq)]
pub enum PermissionLevel {
    ReadOnly,
    ReadWrite,
}

impl PermissionLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            PermissionLevel::ReadOnly => "read-only",
            PermissionLevel::ReadWrite => "read-write",
        }
    }
}

// Cache for authenticated users
lazy_static! {
    static ref USER_CACHE: Arc<DashMap<String, AuthenticatedUser>> = Arc::new(DashMap::new());
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub username: String,
    pub role: String,
    pub user_id: i64,
}

#[derive(Debug)]
pub enum AuthError {
    MissingCredentials,
    InvalidToken,
    InvalidCredentials,
    SignatureMismatch,
    ExpiredTimestamp,
}

#[derive(Debug)]
pub enum AuthType {
    Jwt(String),           // Bearer token
    BasicAuth(String, String), // username, password
    AwsSignatureV4(AwsSignature), // AWS Signature v4
}

#[derive(Debug)]
pub struct AwsSignature {
    pub access_key: String,
    pub signature: String,
    pub timestamp: String,
    pub region: String,
    pub service: String,
}

pub struct AuthExtractor(pub AuthenticatedUser);

impl AuthExtractor {
    pub async fn from_headers(
        headers: &HeaderMap,
        pool: &SqlitePool,
    ) -> Result<Self, AuthError> {
        let auth_header = headers
            .get("authorization")
            .ok_or(AuthError::MissingCredentials)?
            .to_str()
            .map_err(|_| AuthError::InvalidCredentials)?;

        let auth_type = parse_auth_header(auth_header, headers)?;
        let user = validate_auth(auth_type, pool, headers).await?;

        Ok(AuthExtractor(user))
    }
}

fn parse_auth_header(header: &str, headers: &HeaderMap) -> Result<AuthType, AuthError> {
    tracing::info!("Parsing authorization header: {}", header);

    // Check for Bearer token (JWT)
    if header.starts_with(BEARER_PREFIX) {
        let token = header.trim_start_matches(BEARER_PREFIX).trim();
        if token.is_empty() {
            tracing::warn!("Empty JWT token provided");
            return Err(AuthError::InvalidToken);
        }
        return Ok(AuthType::Jwt(token.to_string()));
    }

    // Check for Basic auth
    if header.starts_with(BASIC_PREFIX) {
        let credentials = header.trim_start_matches(BASIC_PREFIX).trim();
        let decoded = BASE64_STANDARD
            .decode(credentials)
            .map_err(|_| {
                tracing::warn!("Failed to decode Basic Auth credentials");
                AuthError::InvalidCredentials
            })?;
        let cred_str = String::from_utf8(decoded).map_err(|_| {
            tracing::warn!("Invalid UTF-8 in Basic Auth credentials");
            AuthError::InvalidCredentials
        })?;

        let parts: Vec<&str> = cred_str.splitn(2, ':').collect();
        if parts.len() != 2 {
            tracing::warn!("Invalid Basic Auth format");
            return Err(AuthError::InvalidCredentials);
        }

        return Ok(AuthType::BasicAuth(
            parts[0].to_string(),
            parts[1].to_string(),
        ));
    }

    // Check for AWS Signature v4
    if header.starts_with(AWS_SIGNATURE_PREFIX) {
        if let Some(signature) = parse_aws_signature(header, headers) {
            return Ok(AuthType::AwsSignatureV4(signature));
        }
        tracing::warn!("Failed to parse AWS Signature v4");
        return Err(AuthError::InvalidCredentials);
    }

    tracing::warn!("Unsupported authorization header format");
    Err(AuthError::InvalidCredentials)
}

fn parse_aws_signature(header: &str, headers: &HeaderMap) -> Option<AwsSignature> {
    let parts: Vec<&str> = header.split(',').collect();
    if parts.len() < 2 {
        tracing::warn!("Invalid AWS Signature v4 header format");
        return None;
    }

    let credential_part = parts.iter().find(|p| p.contains("Credential="))?;
    let signature_part = parts.iter().find(|p| p.contains("Signature="))?;

    let credential = credential_part.trim_start_matches("Credential=").trim();
    let signature = signature_part.trim_start_matches("Signature=").trim();

    let cred_parts: Vec<&str> = credential.split('/').collect();
    if cred_parts.len() < 5 {
        tracing::warn!("Invalid AWS Signature v4 credential format");
        return None;
    }

    let timestamp = headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            tracing::warn!("Missing or invalid x-amz-date header");
        })
        .ok()?;

    Some(AwsSignature {
        access_key: cred_parts[0].to_string(),
        signature: signature.to_string(),
        timestamp,
        region: cred_parts[2].to_string(),
        service: cred_parts[3].to_string(),
    })
}

async fn validate_auth(
    auth_type: AuthType,
    pool: &SqlitePool,
    headers: &HeaderMap,
) -> Result<AuthenticatedUser, AuthError> {
    tracing::info!("Validating authentication type: {:?}", auth_type);
    let result = match auth_type {
        AuthType::Jwt(token) => validate_jwt(&token)
            .map_err(|e| {
                tracing::warn!("JWT validation failed: {}", e);
                AuthError::InvalidToken
            }),
        AuthType::BasicAuth(username, password) => validate_credentials(pool, &username, &password).await,
        AuthType::AwsSignatureV4(signature) => validate_aws_signature(signature, pool, headers).await,
    };

    if result.is_ok() {
        tracing::info!("Authentication successful");
    } else {
        tracing::warn!("Authentication failed: {:?}", result);
    }
    result
}

async fn validate_credentials(
    pool: &SqlitePool,
    username: &str,
    password: &str,
) -> Result<AuthenticatedUser, AuthError> {
    // Check cache first
    if let Some(cached_user) = USER_CACHE.get(username) {
        let user = sqlx::query_as::<_, User>(
            "SELECT id, username, password_hash, role FROM users WHERE username = ?"
        )
        .bind(username)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            tracing::error!("Database error for cached user: {}", e);
            AuthError::InvalidCredentials
        })?;

        if let Some(user) = user {
            if verify(password, &user.password_hash).map_err(|e| {
                tracing::error!("Failed to verify password for user {}: {}", username, e);
                AuthError::InvalidCredentials
            })? {
                tracing::info!("User {} authenticated from cache", username);
                return Ok(cached_user.clone());
            }
        }
    }

    match verify_credentials(pool, username, password).await {
        Ok(Some(user)) => {
            let auth_user = AuthenticatedUser {
                username: user.username.clone(),
                role: user.role.clone(),
                user_id: user.id,
            };
            USER_CACHE.insert(username.to_string(), auth_user.clone());
            tracing::info!("User {} authenticated and cached", username);
            Ok(auth_user)
        }
        Ok(None) => {
            tracing::warn!("Invalid credentials for user: {}", username);
            Err(AuthError::InvalidCredentials)
        }
        Err(e) => {
            tracing::error!("Database error during authentication: {}", e);
            Err(AuthError::InvalidCredentials)
        }
    }
}

async fn validate_aws_signature(
    signature: AwsSignature,
    pool: &SqlitePool,
    _headers: &HeaderMap,
) -> Result<AuthenticatedUser, AuthError> {
    // Validate timestamp (within 15 minutes)
    let timestamp = DateTime::parse_from_rfc3339(&signature.timestamp)
        .map_err(|_| {
            tracing::warn!("Invalid x-amz-date format: {}", signature.timestamp);
            AuthError::InvalidCredentials
        })?
        .with_timezone(&Utc);
    let now = Utc::now();
    if (now - timestamp).abs() > Duration::minutes(15) {
        tracing::warn!("AWS Signature timestamp expired or invalid");
        return Err(AuthError::ExpiredTimestamp);
    }

    // Retrieve user from database
    let user = sqlx::query_as::<_, User>(
        "SELECT id, username, role FROM users WHERE username = ?"
    )
    .bind(&signature.access_key)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!("Database error for AWS auth: {}", e);
        AuthError::InvalidCredentials
    })?;

    if let Some(user) = user {
        // Placeholder for AWS Signature v4 verification
        tracing::info!("AWS Signature verification placeholder for user: {}", user.username);
        Ok(AuthenticatedUser {
            username: user.username,
            role: user.role,
            user_id: user.id,
        })
    } else {
        tracing::warn!("No user found for access key: {}", signature.access_key);
        Err(AuthError::InvalidCredentials)
    }
}

pub async fn authenticate_request(
    headers: &HeaderMap,
    pool: &SqlitePool,
) -> Result<AuthenticatedUser, AppError> {
    AuthExtractor::from_headers(headers, pool)
        .await
        .map(|extractor| extractor.0)
        .map_err(|e| {
            tracing::error!("Authentication error: {:?}", e);
            match e {
                AuthError::MissingCredentials => AppError::AccessDenied,
                AuthError::InvalidToken => AppError::AccessDenied,
                AuthError::InvalidCredentials => AppError::AccessDenied,
                AuthError::SignatureMismatch => AppError::AccessDenied,
                AuthError::ExpiredTimestamp => AppError::AccessDenied,
            }
        })
}

// pub async fn auth_middleware(
//     Extension(pool): Extension<SqlitePool>,
//     mut request: Request<axum::body::Body>,
//     next: Next,
// ) -> Result<Response, Response> {
//     match authenticate_request(request.headers(), &pool).await {
//         Ok(user) => {
//             request.extensions_mut().insert(user);
//             Ok(next.run(request).await)
//         }
//         Err(e) => {
//             tracing::error!("Authentication failed: {:?}", e);
//             Err((StatusCode::UNAUTHORIZED, format!("{:?}", e)).into_response())
//         }
//     }
// }

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, Response> {
    match authenticate_request(request.headers(), &state.pool).await {
        Ok(user) => {
            request.extensions_mut().insert(user);
            Ok(next.run(request).await)
        }
        Err(e) => {
            tracing::error!("Authentication failed: {:?}", e);
            Err((StatusCode::UNAUTHORIZED, format!("{:?}", e)).into_response())
        }
    }
}

pub async fn check_bucket_permission(
    pool: &SqlitePool,
    user: &AuthenticatedUser,
    bucket: &str,
    required_level: &str,
) -> Result<bool, anyhow::Error> {
    tracing::info!(
        "Checking bucket permission for user {} on bucket {} with level {}",
        user.username,
        bucket,
        required_level
    );

    if user.role == "admin" {
        tracing::info!("Admin user {} granted full access to bucket {}", user.username, bucket);
        return Ok(true);
    }

    let permission_level = sqlx::query_scalar::<_, String>(
        "SELECT permission_level FROM user_bucket_permissions 
         WHERE user_id = ? AND bucket_name = ?"
    )
    .bind(user.user_id)
    .bind(bucket)
    .fetch_optional(pool)
    .await
    .context(format!("Failed to fetch bucket permission for user {} and bucket {}", user.username, bucket))?;

    let result = match (required_level, permission_level.as_deref()) {
        (level, Some(perm)) if level == PermissionLevel::ReadOnly.as_str() => {
            perm == PermissionLevel::ReadOnly.as_str() || perm == PermissionLevel::ReadWrite.as_str()
        }
        (level, Some(perm)) if level == PermissionLevel::ReadWrite.as_str() => {
            perm == PermissionLevel::ReadWrite.as_str()
        }
        _ => false,
    };

    tracing::info!(
        "Permission check result for user {} on bucket {}: {}",
        user.username,
        bucket,
        result
    );
    Ok(result)
}