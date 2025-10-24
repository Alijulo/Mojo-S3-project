use sqlx::{SqlitePool, Row};
use std::env;
use anyhow::{Context, bail};
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString, PasswordVerifier},
    Argon2, PasswordHash,
};

// Uses Argon2 for password hashing.
fn hash_password(password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default(); 

    let password_hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .context("Failed to hash password with Argon2")?
        .to_string();

    Ok(password_hash)
}

// Struct to hold user information, including the DB ID for permissions linking.
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct User {
    pub id: i64, // Must fetch the internal DB ID for FK linkage
    pub username: String,
    pub password_hash: String,
    pub role: String,
}


// Function required for JWT authentication: verifies credentials.
pub async fn verify_credentials(
    pool: &SqlitePool, 
    username: &str, 
    password: &str
) -> Result<Option<User>, Box<dyn std::error::Error>> {
    
    // 1. Retrieve user by username
    let user = sqlx::query_as::<_, User>(
        "SELECT id, username, password_hash, role FROM users WHERE username = ?",
    )
    .bind(username)
    .fetch_optional(pool)
    .await?;

    let user = match user {
        Some(u) => u,
        None => return Ok(None), // User not found
    };

    // 2. Verify password against stored hash
    let parsed_hash = PasswordHash::new(&user.password_hash)
        .context("Invalid stored password hash format")?;

    let is_valid = Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok();

    if is_valid {
        Ok(Some(user))
    } else {
        Ok(None) // Password mismatch
    }
}


pub async fn initialize_database(pool: &SqlitePool) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info("Starting database initialization and schema setup...");
    // 1. Create the Object Metadata Table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS object_meta (
            bucket_name      TEXT NOT NULL,
            object_key       TEXT NOT NULL,
            etag             TEXT NOT NULL,
            content_length   INTEGER NOT NULL,
            content_type     TEXT,
            last_modified    TEXT NOT NULL,
            custom_metadata  TEXT,
            PRIMARY KEY (bucket_name, object_key)
        );
        CREATE INDEX IF NOT EXISTS idx_key_prefix ON object_meta (bucket_name, object_key);
        "#,
    ).execute(pool).await?;

    // 2. Create the Users Table for Authentication
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            username         TEXT NOT NULL UNIQUE,
            password_hash    TEXT NOT NULL,
            role             TEXT NOT NULL DEFAULT 'user'
        );
        "#,
    ).execute(pool).await?;
    
    // --- 3. NEW: Create the User Bucket Permissions Table ---
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS user_bucket_permissions (
            user_id              INTEGER NOT NULL,
            bucket_name          TEXT NOT NULL,
            permission_level     TEXT NOT NULL CHECK(permission_level IN ('read-only', 'read-write')),
            PRIMARY KEY (user_id, bucket_name),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        "#,
    ).execute(pool).await?;


    // 4. Check if any users exist
    let count: i64 = sqlx::query_scalar("SELECT COUNT(id) FROM users")
        .fetch_one(pool).await?;

    if count == 0 {
        // 5. Initialize Admin User
        
        let admin_user = env::var("MOJO_ROOT_USER")
            .or_else(|_| env::var("MINIO_ROOT_USER"))
            .unwrap_or_else(|_| {
                eprintln!("MOJO_ROOT_USER not set. Using default 'admin'.");
                "admin".to_string()
            });

        let admin_pass = env::var("MOJO_ROOT_PASSWORD")
            .or_else(|_| env::var("MINIO_ROOT_PASSWORD"))
            .map_err(|_| "MOJO_ROOT_PASSWORD or MINIO_ROOT_PASSWORD must be set in .env")?;

        let hashed_password = hash_password(&admin_pass)?;
        
        tracing::info("Inserting initial admin user: '{}'", admin_user);
        
        // Insert user and get the resulting ID
        let result = sqlx::query(
            r#"
            INSERT INTO users (username, password_hash, role)
            VALUES (?, ?, 'admin')
            "#,
        )
        .bind(&admin_user)
        .bind(&hashed_password)
        .execute(pool)
        .await?;

        let admin_id = result.last_insert_rowid();

        // 6. --- NEW: Grant Admin Initial Permissions ---
        const DEFAULT_ADMIN_BUCKET: &str = "admin-data";
        
        sqlx::query(
            r#"
            INSERT INTO user_bucket_permissions (user_id, bucket_name, permission_level)
            VALUES (?, ?, 'read-write')
            "#,
        )
        .bind(admin_id)
        .bind(DEFAULT_ADMIN_BUCKET)
        .execute(pool)
        .await?;
        
        tracing::info("Admin user setup successful. Granted 'read-write' for bucket '{}'.", DEFAULT_ADMIN_BUCKET);
    } else {
        println!("Users already exist. Skipping admin creation.");
    }

    Ok(())
}


// insert user
pub async fn insert_user(
    pool: &SqlitePool, 
    current_user_id: i64, // The ID of the user attempting to create the new user
    new_username: &str, 
    new_password: &str, 
    new_user_role: &str,
    bucket_permissions: Option<Vec<(&str, &str)>>, // (bucket_name, permission_level)
) -> Result<User, Box<dyn std::error::Error>> {
    
    // --- 1. ENFORCE ADMIN-ONLY USER CREATION ---
    let current_user_role = get_user_role(pool, current_user_id).await?
        .context("Current user not found, cannot create new user.")?;

    if current_user_role != "admin" {
        // Use `bail!` from `anyhow` to return a clear error
        tracing::info("Permission denied: Only 'admin' users can create new accounts. user role: {}",current_user_role);
        bail!("Permission denied: Only 'admin' users can create new accounts.");
    }
    // ------------------------------------------

    // --- 2. HASH PASSWORD AND INSERT USER ---
    let hashed_password = hash_password(new_password)?;
    
    let result = sqlx::query(
        r#"
        INSERT INTO users (username, password_hash, role)
        VALUES (?, ?, ?)
        "#,
    )
    .bind(new_username)
    .bind(&hashed_password)
    .bind(new_user_role)
    .execute(pool)
    .await?;

    let new_user_id = result.last_insert_rowid();

    // Retrieve the newly created user (optional, but useful for a full return value)
    let new_user = sqlx::query_as::<_, User>(
        "SELECT id, username, password_hash, role FROM users WHERE id = ?",
    )
    .bind(new_user_id)
    .fetch_one(pool)
    .await?;

    // --- 3. GRANT INITIAL BUCKET PERMISSIONS (Admin-only right) ---
    if let Some(permissions) = bucket_permissions {
        for (bucket_name, permission_level) in permissions {
            // Because the check above ensures the current_user is an 'admin',
            // this step effectively enforces the admin's right to assign permissions.
            sqlx::query(
                r#"
                INSERT INTO user_bucket_permissions (user_id, bucket_name, permission_level)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, bucket_name) DO UPDATE SET permission_level = excluded.permission_level
                "#,
            )
            .bind(new_user_id)
            .bind(bucket_name)
            .bind(permission_level)
            .execute(pool)
            .await
            .context(format!("Failed to grant bucket permission for bucket: {}", bucket_name))?;
        }
    }
    tracing::info("New user created successfully {}", new_username);
    Ok(new_user)
}