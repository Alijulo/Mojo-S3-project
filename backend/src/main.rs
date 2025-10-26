use anyhow::Context;
use std::io::ErrorKind;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use tokio::fs;
use axum::{
    response::{IntoResponse, Response},
    routing::{get, post},
    extract::{State},
    Router,
    http::{StatusCode, HeaderMap},
    middleware,
};
use headers::Authorization;
use axum_extra::TypedHeader;
use thiserror::Error;
use tracing::{error, info, Level};
use sqlx::{SqlitePool};

// S3 XML Serialization Imports
use serde::{Serialize, Deserialize};
use quick_xml::se::to_string as to_xml_string;
use tower_http::cors::{CorsLayer, Any};
use std::env::current_dir;
// Module Declarations
mod s3_operations;


// Re-export handler functions
use s3_operations::bucket_handlers::{get_all_buckets, put_bucket, delete_bucket, head_bucket};
use s3_operations::object_handlers::{put_object, get_object, list_objects, delete_object, head_object};
use s3_operations::handler_utils::{S3Error, S3Headers};
use s3_operations::auth::{auth_middleware,};
use s3_operations::user_models::{initialize_database,verify_credentials};
use s3_operations::jwt_utils::generate_jwt;

// --- Shared S3-Standard XML Response Structs ---
pub const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

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

// --- List Buckets (GET /) ---
#[derive(Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
pub struct ListAllMyBucketsResult {
    #[serde(rename = "@xmlns")]
    pub xmlns: &'static str,
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "Buckets")]
    pub buckets: Buckets,
}

#[derive(Serialize)]
pub struct Owner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

#[derive(Serialize)]
pub struct Buckets {
    #[serde(rename = "Bucket")]
    pub bucket: Vec<BucketInfo>,
}

#[derive(Serialize)]
pub struct BucketInfo {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "CreationDate")]
    pub creation_date: String,
}

// --- List Objects (GET /{bucket}) ---
#[derive(Serialize)]
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

#[derive(Serialize)]
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
            AppError::AccessDenied => (
                StatusCode::FORBIDDEN,
                S3Error::access_denied("resource")
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

// --- Application State ---
#[derive(Clone)]
pub struct AppState {
    pub storage_root: PathBuf,
    pub pool: SqlitePool,
}

impl AppState {
    pub fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.storage_root.join(bucket).join(key)
    }

    pub fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.storage_root.join(bucket)
    }
}

// // --- Authentication Handler (Login) - Returns XML ---
// async fn login_handler(
//     State(state): State<Arc<AppState>>,
//     Json(payload): Json<AuthRequest>,
// ) -> Result<(HeaderMap, String), AppError> {
//     match verify_credentials(&state.pool, &payload.username, &payload.password).await {
//         Ok(Some(user)) => {
//             // Use jwt_utils::generate_jwt
//             let token = generate_jwt(&user)
//                 .map_err(|e| {
//                     tracing::error!("Failed to create JWT token: {}", e);
//                     AppError::Internal(anyhow::anyhow!("Token creation failed"))
//                 })?;
            
//             // Return XML response
//             let response_data = AuthResponseXml {
//                 token,
//                 username: user.username,
//                 role: user.role,
//                 user_id: user.id,
//             };

//             let xml_body = to_xml_string(&response_data)
//                 .context("Failed to serialize auth response to XML")?;

//             let headers = S3Headers::xml_headers();
//             Ok((headers, xml_body))
//         }
//         Ok(None) => {
//             Err(AppError::AccessDenied) 
//         }
//         Err(e) => {
//             Err(AppError::Internal(anyhow::anyhow!("Database error: {}", e)))
//         }
//     }
// }

// Authentication Handler
async fn login_handler(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<headers::authorization::Basic>>, // Fixed
) -> Result<(HeaderMap, String), AppError> {
    let username = auth.username().to_string();
    let password = auth.password().to_string();

    match verify_credentials(&state.pool, &username, &password).await {
        Ok(Some(user)) => {
            let token = generate_jwt(&user)
                .map_err(|e| {
                    tracing::error!("Failed to create JWT token: {}", e);
                    AppError::Internal(anyhow::anyhow!("Token creation failed"))
                })?;

            let response_data = AuthResponseXml {
                token,
                username: user.username,
                role: user.role,
                user_id: user.id,
            };

            let xml_body = to_xml_string(&response_data)
                .context("Failed to serialize auth response to XML")?;

            let headers = S3Headers::xml_headers();
            Ok((headers, xml_body))
        }
        Ok(None) => Err(AppError::AccessDenied),
        Err(e) => Err(AppError::Internal(anyhow::anyhow!("Database error: {}", e))),
    }
}

// --- Main Entry Point ---
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("Starting Mojo S3 Server...");

    // // Database Setup
    // let database_url = std::env::var("DATABASE_URL")
    //     .unwrap_or_else(|_| {
    //         info!("DATABASE_URL not set. Using default 'sqlite:data/app.db'.");
    //         "sqlite:data/app.db".to_string()
    //     });
        
    // let pool = SqlitePool::connect(&database_url).await
    //     .context(format!("Failed to connect to database at: {}", database_url))?;


    // Database Setup
    // dotenvy::dotenv().ok();
    info!("Current working directory: {}", current_dir()?.display());
    dotenvy::dotenv().context("Failed to load .env file")?;

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| {
            let db_path = current_dir()
                .expect("Failed to get current directory")
                .join("data")
                .join("app.db");
            let db_path_str = db_path
                .to_str()
                .expect("Database path contains invalid UTF-8 characters")
                .replace("\\", "/");
            let db_url = format!("sqlite://{}?mode=rwc", db_path_str);
            info!("DATABASE_URL not set. Using default '{}'.", db_url);
            db_url
        });

    // Validate database URL
    if !database_url.starts_with("sqlite:") {
        return Err(anyhow::anyhow!("Invalid DATABASE_URL: must start with 'sqlite:'"));
    }

    // Extract and ensure the database directory exists
    let db_file = Path::new(&database_url)
        .strip_prefix("sqlite://")
        .map(Path::new)
        .unwrap_or_else(|_| Path::new("data/app.db"));
    let db_dir = db_file
        .parent()
        .unwrap_or_else(|| Path::new("."));
    info!("Attempting to create database directory: {}", db_dir.display());
    fs::create_dir_all(&db_dir).await
        .context(format!("Failed to create database directory: {}", db_dir.display()))?;
    if db_dir.exists() {
        info!("Database directory created successfully: {}", db_dir.display());
    } else {
        error!("Directory creation failed (no error reported): {}", db_dir.display());
        return Err(anyhow::anyhow!("Directory creation failed: {}", db_dir.display()));
    }

    // Check if directory is writable
    let metadata = fs::metadata(&db_dir).await
        .context("Failed to get database directory metadata")?;
    if metadata.permissions().readonly() {
        error!("Database directory is not writable: {}", db_dir.display());
        return Err(anyhow::anyhow!("Database directory is not writable: {}", db_dir.display()));
    }
    info!("Database directory is writable: {}", db_dir.display());

    // Log database file existence
    if db_file.exists() {
        info!("Database file exists: {}", db_file.display());
    } else {
        info!("Database file does not exist, will be created: {}", db_file.display());
    }

    info!("Attempting to connect to database: {}", database_url);
    let pool = SqlitePool::connect(&database_url).await
        .context(format!("Failed to connect to database at: {}", database_url))?;
    info!("Database connection successful!");

    info!("Initializing database...");
    initialize_database(&pool).await?;
    info!("Database initialized successfully.");

    // Storage Setup
    let storage_root = Path::new("./s3_data").to_path_buf();
    fs::create_dir_all(&storage_root).await
        .context("Failed to create storage root directory")?;
    info!("Local Storage Root set to: {}", storage_root.display());

    let state = Arc::new(AppState { storage_root, pool });

    // CORS Configuration
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Router Setup
    let app = Router::new()
        .route("/api/v1/login", post(login_handler))
        .route("/api/v1/buckets", get(get_all_buckets))
        .route(
            "/api/v1/bucket/{bucket}",
            get(list_objects)
                .put(put_bucket)
                .delete(delete_bucket)
                .head(head_bucket)
        )
        .route(
            "/api/v1/bucket/{bucket}/{key}",
            get(get_object)
                .put(put_object)
                .delete(delete_object)
                .head(head_object)
        )
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state)
        .layer(cors);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .context("Failed to bind TCP listener")?;

    info!("Listening on http://127.0.0.1:3000");
    
    axum::serve(listener, app)
        .await
        .context("Axum server failed")?;

    Ok(())
}