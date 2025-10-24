use jsonwebtoken::{encode, decode, Header, EncodingKey, DecodingKey, Validation, Algorithm};
use serde::{Deserialize, Serialize};
use chrono::Utc;
use axum::{
    extract::{State, FromRequestParts},
    http::{request::Parts, StatusCode},
    response::{Response, IntoResponse},
};
use std::env;
use anyhow::Context;
use sqlx::{SqlitePool, Row}; // Added sqlx for the authorization check
use crate::admin_db_init::User; // Assuming User struct is exported from admin_db_init

// --- CONSTANTS ---
const EXPIRATION_DURATION_HOURS: i64 = 24;

// --- 1. JWT Claims Struct ---
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String, // Subject (username)
    pub role: String, // User role (e.g., "admin", "user")
    pub exp: i64,    // Expiration time (seconds since UNIX epoch)
    pub iat: i64,    // Issued at time
}

// --- 2. Token Generation (Unchanged) ---
pub fn create_token(username: &str, role: &str) -> Result<String, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let expiration = now + chrono::Duration::hours(EXPIRATION_DURATION_HOURS);
    
    let secret = env::var("JWT_SECRET").context("JWT_SECRET environment variable not set. Please add it to your .env file.")?;

    let claims = Claims {
        sub: username.to_string(),
        role: role.to_string(),
        iat: now.timestamp(),
        exp: expiration.timestamp(),
    };

    let header = Header::new(Algorithm::HS256); 
    
    let token = encode(
        &header, 
        &claims, 
        &EncodingKey::from_secret(secret.as_bytes())
    )?;

    Ok(token)
}

// --- 3. Token Validation and Extractor (Unchanged) ---
// This struct provides the authenticated user's claims to any protected Axum route.
pub struct CurrentUser(pub Claims);

#[axum::async_trait]
impl<S> FromRequestParts<S> for CurrentUser
where
    S: Send + Sync,
{
    type Rejection = Response; 

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let secret = env::var("JWT_SECRET")
            .unwrap_or_else(|_| {
                eprintln!("SECURITY ERROR: JWT_SECRET missing in environment. Using mock secret.");
                "MOCK_SECRET_FALLBACK_DO_NOT_USE_IN_PROD".to_string() 
            });
        
        let auth_header = parts.headers
            .get("Authorization")
            .and_then(|value| value.to_str().ok());

        let token = if let Some(header) = auth_header {
            if header.starts_with("Bearer ") {
                header.trim_start_matches("Bearer ").to_owned()
            } else {
                return Err(StatusCode::UNAUTHORIZED.into_response());
            }
        } else {
            return Err(StatusCode::UNAUTHORIZED.into_response());
        };

        match decode::<Claims>(
            &token,
            &DecodingKey::from_secret(secret.as_bytes()),
            &Validation::new(Algorithm::HS256),
        ) {
            Ok(token_data) => Ok(CurrentUser(token_data.claims)),
            Err(_) => Err(StatusCode::UNAUTHORIZED.into_response()), 
        }
    }
}


// --- 4. Authorization Check Function (NEW) ---

pub async fn check_bucket_permission(
    pool: &SqlitePool, 
    user: &User, 
    bucket: &str, 
    required_level: &str
) -> Result<bool, sqlx::Error> {
    
    // Admins always have full access, bypassing specific bucket permissions
    if user.role == "admin" {
        return Ok(true);
    }
    
    let query_result = sqlx::query_scalar::<_, String>(
        "SELECT permission_level FROM user_bucket_permissions 
         WHERE user_id = ? AND bucket_name = ?"
    )
    .bind(user.id)
    .bind(bucket)
    .fetch_optional(pool)
    .await?;

    // Check if permission exists and meets the required level
    if let Some(level) = query_result {
        match required_level {
            // Read-only access is sufficient for LIST/GET operations
            "read-only" => Ok(level == "read-only" || level == "read-write"), 
            
            // Read-write access is required for PUT/DELETE operations
            "read-write" => Ok(level == "read-write"),
            
            _ => Ok(false), // Unknown required level
        }
    } else {
        // No explicit permission found for this user/bucket
        Ok(false)
    }
}
