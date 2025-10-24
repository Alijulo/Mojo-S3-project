use anyhow::Context; 
use std::io::ErrorKind;
use std::path::{PathBuf, Path};
use tokio::fs;
use axum::{
    response::{IntoResponse, Response},
    routing::{get},
    Router,
    http::{StatusCode},
};
use thiserror::Error;
use tracing::{error, info, Level};



// S3 XML Serialization Imports
use serde::Serialize;
use quick_xml::se::to_string as to_xml_string;
use tower_http::cors::{CorsLayer, Any};

// --- Module Declarations ---
// The compiler now looks for the s3_operations module directory (s3_operations/)
// which should contain a mod.rs or the individual handler files.
mod s3_operations; 

// Re-export handler functions for use in routing.
// We must now access them through the parent 's3_operations' module.
use s3_operations::bucket_handlers::{get_all_buckets, list_objects, put_bucket, delete_bucket,head_bucket};
use s3_operations::object_handlers::{put_object, get_object, delete_object,head_object};

// 1. Import modules and structs from your files
use s3_operations::admin_user::{initialize_database, verify_credentials, User};
use s3_operations::jwt_validation_utils::{create_token, CurrentUser};

// --- 0. Shared S3-Standard XML Response Structs (Made Public) ---

pub const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";


// --- JWT Authentication Request/Response Structures ---

#[derive(Deserialize)]
struct AuthRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct AuthResponse {
    token: String,
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

// New struct for the CommonPrefixes element (S3 "folders")
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
    
    // ADDED: Query parameters echoed back
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
    
    // ADDED: CommonPrefixes for delimiter grouping
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
    
    // ADDED: ETag field
    #[serde(rename = "ETag")]
    pub etag: String,
    
    #[serde(rename = "Size")]
    pub size_bytes: u64,
}


// --- 1. S3-Oriented Custom Error Handling (Made Public) ---

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Storage I/O Error: {0}")]
    Io(#[from] tokio::io::Error),

    #[error("Path not found: {0}")]
    NotFound(String),

    #[error("Internal Server Error: {0}")]
    Internal(#[source] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("Application Error: {:?}", self);

        let (status, message) = match self {
            AppError::Io(e) => {
                match e.kind() {
                    ErrorKind::NotFound => (StatusCode::NOT_FOUND, format!("Object not found on disk.")),
                    ErrorKind::DirectoryNotEmpty | ErrorKind::Other | ErrorKind::PermissionDenied => {
                        (StatusCode::CONFLICT, format!("Bucket must be empty before deletion: {}", e))
                    }
                    _ => (StatusCode::INTERNAL_SERVER_ERROR, format!("Disk access failed: {}", e)),
                }
            },
            AppError::NotFound(path) => (StatusCode::NOT_FOUND, format!("Bucket/Object Not Found: {}", path)),
            AppError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Internal logic error: {}", e)),
        };

        (status, message).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(error: anyhow::Error) -> Self {
        AppError::Internal(error)
    }
}


// --- 2. Application State (Made Public) ---

#[derive(Clone)]
pub struct AppState {
    pub storage_root: PathBuf,
    pub pool: SqlitePool,
}

impl AppState {
    /// Returns the full PathBuf for a given object key within a bucket.
    pub fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.storage_root.join(bucket).join(key)
    }

    /// Returns the full PathBuf for a given bucket directory.
    pub fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.storage_root.join(bucket)
    }
}


// --- 3. Authentication Handler (Login) ---

/// Handles user login, verifies credentials, and returns a JWT.
async fn login_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<AuthRequest>,
) -> Result<Json<AuthResponse>, AppError> { // Use AppError for unified error handling
    
    // 3. Use verify_credentials to check the username and password
    match verify_credentials(&state.pool, &payload.username, &payload.password).await {
        Ok(Some(user)) => {
            // Credentials are valid, create the token
            let token = create_token(&user.username, &user.role)
                .context("Failed to create JWT token")?;
            
            // Return the token to the client
            Ok(Json(AuthResponse { token }))
        }
        Ok(None) => {
            // User not found or password mismatch - return UNAUTHORIZED/Access Denied
            Err(AppError::AccessDenied) 
        }
        Err(e) => {
            // Database or Argon2 error - propagate as internal
            Err(AppError::Internal(e.into()))
        }
    }
}

//===========================
// --- 4. Main Entry Point ---
//============================

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Initialize tracing for structured logging and crash visibility
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("Starting Mojo S3 Server...");
    // --- Database Setup ---
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| {
            info!("DATABASE_URL not set. Using default 'sqlite:data/app.db'.");
            "sqlite:data/app.db".to_string()
        });
        
    let pool = SqlitePool::connect(&database_url).await
        .context(format!("Failed to connect to database at: {}", database_url))?;

    // Initialize database schema and admin user
    initialize_database(&pool).await?;

    info!("Database initialized successfully.");

    // Define the root directory for storage.
    let storage_root = Path::new("./s3_data").to_path_buf();
    fs::create_dir_all(&storage_root).await.context("Failed to create storage root directory")?;
    
    info!("Local Storage Root set to: {}", storage_root.display());

    //let state = AppState { storage_root };
    // --- Application State ---
    let state = Arc::new(AppState { storage_root, pool });

    // Configure CORS Layer to allow requests from any origin (or specify a frontend URL)
    let cors = CorsLayer::new()
        .allow_origin(Any) // For development, allow any origin (e.g., http://localhost:5173)
        .allow_methods(Any) // Allow all HTTP methods (GET, PUT, DELETE)
        .allow_headers(Any); // Allow all headers (including Content-Type)

    // Define routes using the imported handler functions:
    // Prefix all routes with /api/v1
let app = Router::new()
    // Buckets endpoints
    .route("/api/v1/buckets", get(get_all_buckets)) // GET /api/v1/buckets - List all buckets
    .route(
        "/api/v1/bucket/{bucket}",
        get(list_objects)       // GET /api/v1/bucket/{bucket} - List objects in bucket
        .put(put_bucket)        // PUT /api/v1/bucket/{bucket} - Create/update bucket
        .delete(delete_bucket)// DELETE /api/v1/bucket/{bucket} - Delete bucket
        .head(head_bucket)  // <-- HEAD /api/v1/bucket/{bucket} - Check bucket existence
    )
    // Object endpoints
    .route(
        "/api/v1/bucket/{bucket}/{key}",
        get(get_object)         // GET /api/v1/bucket/{bucket}/{key} - Download object
        .put(put_object)        // PUT /api/v1/bucket/{bucket}/{key} - Upload object
        .delete(delete_object)  // DELETE /api/v1/bucket/{bucket}/{key} - Delete object
        .head(head_object) // <-- HEAD /api/v1/bucket/{bucket}/{key} - Get object metadata
    )
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
