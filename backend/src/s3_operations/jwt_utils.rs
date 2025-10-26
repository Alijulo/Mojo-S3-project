use jsonwebtoken::{encode, decode, Header, EncodingKey, DecodingKey, Validation, Algorithm};
use serde::{Deserialize, Serialize};
use chrono::{Utc, Duration};
use anyhow::{Context, Result};
use crate::s3_operations::auth::AuthenticatedUser;
use crate::s3_operations::user_models::User; // Assuming this is the same as s3_operations::user_models::User

const EXPIRATION_DURATION_HOURS: i64 = 24;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,    // Subject (username)
    pub role: String,   // User role (e.g., "admin", "user")
    pub user_id: i64,   // User ID
    pub exp: i64,       // Expiration time (seconds since UNIX epoch)
    pub iat: i64,       // Issued at time
}

/// Generate a JWT token for a user
pub fn generate_jwt(user: &User) -> Result<String> {
    let secret = std::env::var("JWT_SECRET")
        .context("JWT_SECRET environment variable not set")?;

    let now = Utc::now();
    let claims = Claims {
        sub: user.username.clone(),
        role: user.role.clone(),
        user_id: user.id,
        exp: (now + Duration::hours(EXPIRATION_DURATION_HOURS)).timestamp(),
        iat: now.timestamp(),
    };

    let header = Header::new(Algorithm::HS256);
    encode(&header, &claims, &EncodingKey::from_secret(secret.as_ref()))
        .context("Failed to encode JWT token")
}

/// Validate a JWT token and return the authenticated user
pub fn validate_jwt(token: &str) -> Result<AuthenticatedUser> {
    let secret = std::env::var("JWT_SECRET")
        .context("JWT_SECRET environment variable not set")?;

    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_ref()),
        &Validation::new(Algorithm::HS256),
    )
    .context("Failed to decode JWT token")?;

    let now = Utc::now().timestamp();
    if token_data.claims.exp < now {
        tracing::warn!("JWT token expired for user: {}", token_data.claims.sub);
        return Err(anyhow::anyhow!("JWT token expired"));
    }

    Ok(AuthenticatedUser {
        username: token_data.claims.sub,
        role: token_data.claims.role,
        user_id: token_data.claims.user_id,
    })
}