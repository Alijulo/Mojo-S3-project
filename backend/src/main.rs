// main.rs

use anyhow::{anyhow, Context, Result};
use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;
use axum::{
    extract::{DefaultBodyLimit, State,},
    http::{HeaderMap, StatusCode},
    middleware,
    response::IntoResponse,
    routing::{delete, get, head, post, put},
    Router,
};
use headers::Authorization;
use axum_extra::TypedHeader;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, Level};
use tracing_subscriber;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use dashmap::DashMap;
use tokio::sync::{Mutex, Semaphore,OwnedMutexGuard};
use once_cell::sync::Lazy;

// --- Local modules ---



mod s3_operations;
use s3_operations::bucket_handlers::{
    delete_bucket, get_all_buckets, get_bucket_metadata, head_bucket, put_bucket_no_subpath,
    put_bucket_with_subpath,
};
use s3_operations::object_handlers::{
    delete_object, get_object, head_object, list_objects, put_object,presign_object
};
use s3_operations::handler_utils::{S3Headers};
use s3_operations::background_workers::{
    BackgroundWorkers,spawn_fsync_worker,spawn_archive_copy_worker,spawn_bucket_meta_worker};
use s3_operations::auth::auth_middleware;
use s3_operations::user_models::{initialize_database, verify_credentials};
use s3_operations::jwt_utils::generate_jwt;
use s3_operations::{store,index};
use store::{ClusterStore, LocalStore, ObjectStore};
use index::BucketIndex;
// --- Shared XML response structs ---
use serde::{Deserialize, Serialize};
use quick_xml::se::to_string as to_xml_string;

// --- Global I/O semaphore ---
static GLOBAL_IO_SEMAPHORE: Lazy<Semaphore> = Lazy::new(|| Semaphore::new(32));
// --- Shared S3-Standard XML Response Structs ---
pub const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";



// --- Per-key async locks ---
#[derive(Default)]
pub struct IoLocks {
    locks: DashMap<String, Arc<Mutex<()>>>,
}

impl IoLocks {
    pub fn new() -> Self {
        Self { locks: DashMap::new() }
    }

    pub async fn lock(&self, key: String) -> OwnedMutexGuard<()> {
        let entry = self
            .locks
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())));
        let arc = entry.value().clone();
        arc.lock_owned().await
    }
}

// Configurable durability level
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityLevel {
    Strong,   // fsyncs enabled
    Relaxed,  // rely on background periodic fsync/repair
}

impl DurabilityLevel {
    pub fn from_env() -> Self {
        match std::env::var("DURABILITY_LEVEL").as_deref() {
            Ok("RELAXED") => DurabilityLevel::Relaxed,
            _ => DurabilityLevel::Strong,
        }
    }
}


// --- Application State ---
#[derive(Clone)]
pub struct AppState {
    pub storage_root: PathBuf,
    pub pool: SqlitePool,
    pub store: Arc<dyn ObjectStore>,
    pub io_locks: Arc<IoLocks>,
    pub io_budget: Arc<Semaphore>, // global I/O concurrency budget
    pub durability: DurabilityLevel,              // NEW: durability mode
    pub workers: Option<BackgroundWorkers>,       // NEW: background workers
    //per-bucket indexes
    pub indexes: Arc<DashMap<String, Arc<BucketIndex>>>,
}

impl AppState {
    // Path helpers for consistent, safe path construction
    pub fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.storage_root.join(bucket).join(key)
    }
    pub fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.storage_root.join(bucket)
    }
    pub async fn get_bucket_index(&self, bucket: &str) -> Result<Arc<BucketIndex>> {
        if let Some(idx) = self.indexes.get(bucket) {
            return Ok(idx.clone());
        }

        // Lazily open/create index if not cached
        let base = self.bucket_path(bucket);
        let idx = BucketIndex::open(bucket, &base).await?;
        let arc_idx = Arc::new(idx);
        self.indexes.insert(bucket.to_string(), arc_idx.clone());
        Ok(arc_idx)
    }
}


// --- Authentication Response ---
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



// --- Authentication Handler ---
async fn login_handler(
    State(state): State<Arc<AppState>>,
    TypedHeader(auth): TypedHeader<Authorization<headers::authorization::Basic>>,
) -> Result<(HeaderMap, String), s3_operations::handler_utils::AppError> {
    let username = auth.username().to_string();
    let password = auth.password().to_string();

    match verify_credentials(&state.pool, &username, &password).await {
        Ok(Some(user)) => {
            let token = generate_jwt(&user).map_err(|e| {
                error!("Failed to create JWT token: {}", e);
                s3_operations::handler_utils::AppError::Internal(anyhow!("Token creation failed"))
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
        Ok(None) => Err(s3_operations::handler_utils::AppError::AccessDenied),
        Err(e) => Err(s3_operations::handler_utils::AppError::Internal(anyhow!(
            "Database error: {}",
            e
        ))),
    }
}

// --- Main Entry Point ---
#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Mojo S3 Server...");

    // Database Setup
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        let db_path = std::env::current_dir()
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

    if !database_url.starts_with("sqlite:") {
        return Err(anyhow!("Invalid DATABASE_URL: must start with 'sqlite:'"));
    }

    let db_file = Path::new(&database_url)
        .strip_prefix("sqlite://")
        .map(Path::new)
        .unwrap_or_else(|_| Path::new("data/app.db"));
    let db_dir = db_file.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(&db_dir)
        .await
        .context(format!("Failed to create database directory: {}", db_dir.display()))?;

    let pool = SqlitePoolOptions::new()
        .max_connections(env::var("DB_MAX_CONN").ok().and_then(|v| v.parse().ok()).unwrap_or(10))
        .connect(&database_url)
        .await
        .context(format!("Failed to connect to database at: {}", database_url))?;
    initialize_database(&pool).await?;

    // Storage Setup
    let storage_root = Path::new(&env::var("STORAGE_ROOT").unwrap_or_else(|_| "./s3_data".into()))
        .to_path_buf();
    fs::create_dir_all(&storage_root)
        .await
        .context("Failed to create storage root directory")?;

    // Choose store implementation
    let store: Arc<dyn ObjectStore> = if let Ok(nodes_csv) = env::var("CLUSTER_NODES") {
        let nodes: Vec<String> = nodes_csv
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if nodes.is_empty() {
            Arc::new(LocalStore::new(storage_root.clone()))
        } else {
            Arc::new(ClusterStore::new(nodes).await?)
        }
    } else {
        Arc::new(LocalStore::new(storage_root.clone()))
    };

    // --- Global I/O budget (Semaphore) ---
    let io_budget_permits = env::var("IO_BUDGET")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(64); // default concurrency limit
    let io_budget = Arc::new(Semaphore::new(io_budget_permits));

    // --- Durability mode ---
    let durability = DurabilityLevel::from_env();

    // --- Background worker channels ---
    let (bucket_meta_tx, bucket_meta_rx) = tokio::sync::mpsc::channel(100);
    let (fsync_tx, fsync_rx) = tokio::sync::mpsc::channel(100);
    let (archive_copy_tx, archive_copy_rx) = tokio::sync::mpsc::channel(100);

    // Spawn workers
    spawn_bucket_meta_worker(bucket_meta_rx, storage_root.clone(), durability);
    spawn_fsync_worker(fsync_rx);
    spawn_archive_copy_worker(archive_copy_rx);

    let workers = BackgroundWorkers {
        bucket_meta_tx,
        fsync_tx,
        archive_copy_tx,
    };


    // --- Application State ---
    let state = Arc::new(AppState {
        storage_root,
        pool,
        store,
        io_locks: Arc::new(IoLocks::new()),
        io_budget, // NEW field
        durability,                // NEW
        workers: Some(workers),
        indexes: Arc::new(DashMap::new()),
    });

    // CORS
    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);

    // Body size limit
    let body_limit = DefaultBodyLimit::max(
        env::var("MAX_BODY_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10 * 1024 * 1024 * 1024),
    );

    // Router
    let app = Router::new()
        .route("/api/v1/login", post(login_handler))
        .route("/api/v1/buckets", get(get_all_buckets))
        .route(
            "/api/v1/bucket/{bucket}",
            get(list_objects)
                .put(put_bucket_no_subpath)
                .delete(delete_bucket)
                .head(head_bucket),
        )
        .route("/api/v1/bucket/{bucket}/metadata", get(get_bucket_metadata))
        .route("/api/v1/bucket/{bucket}/{*subpath}", put(put_bucket_with_subpath))
        .route(
            "/api/v1/object/{bucket}/{key}",
            get(get_object)
                .put(put_object)
                .delete(delete_object)
                .head(head_object),
        )
        .route(
        "/api/v1/object/{bucket}/{key}/presign",
        get(presign_object),
        )
        .layer(body_limit)
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        .layer(cors)
        .with_state(state);

    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(3000);
    let addr = format!("{}:{}", host, port);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context("Failed to bind TCP listener")?;

    info!("Listening on http://{}", addr);

    axum::serve(listener, app).await.context("Axum server failed")?;
    Ok(())
}

