use sqlx::{SqlitePool, Row};
use anyhow::{Context, Result};
use bcrypt::{hash, verify, DEFAULT_COST};
use tracing::info;

// Replace std::env with dotenvy
use dotenvy::var;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub password_hash: String,
    pub role: String,
}

pub async fn initialize_database(pool: &SqlitePool) -> Result<()> {
    info!("Starting database initialization and schema setup...");

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
    )
    .execute(pool)
    .await
    .context("Failed to create object_meta table")?;

    // 2. Create the Users Table for Authentication
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            username         TEXT NOT NULL UNIQUE,
            password_hash    TEXT NOT NULL,
            role             TEXT NOT NULL DEFAULT 'user'
        );
        CREATE INDEX IF NOT EXISTS idx_username ON users (username);
        "#,
    )
    .execute(pool)
    .await
    .context("Failed to create users table")?;

    // 3. Create the User Bucket Permissions Table
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
    )
    .execute(pool)
    .await
    .context("Failed to create user_bucket_permissions table")?;

    // 4. Check if any users exist
    let count: (i64,) = sqlx::query_as("SELECT COUNT(id) FROM users")
        .fetch_one(pool)
        .await
        .context("Failed to count users")?;

    if count.0 == 0 {
        // 5. Initialize Admin User
        let admin_user = var("MOJO_ROOT_USER")
            .unwrap_or_else(|_| {
                eprintln!("MOJO_ROOT_USER not set. Using default 'admin'.");
                "admin".to_string()
            });

        let admin_pass = var("MOJO_ROOT_PASSWORD")
            .map_err(|_| anyhow::anyhow!("MOJO_ROOT_PASSWORD must be set in .env"))?;

        // Validate admin username and password
        if admin_user.trim().is_empty() {
            return Err(anyhow::anyhow!("Admin username cannot be empty"));
        }
        if admin_pass.trim().is_empty() {
            return Err(anyhow::anyhow!("Admin password cannot be empty"));
        }

        let hashed_password = hash(&admin_pass, DEFAULT_COST)
            .context("Failed to hash admin password")?;
        
        info!("Inserting initial admin user: '{}',{}", admin_user,admin_pass);
        info!("Hashed password: '{}'", hashed_password);
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
        .await
        .context("Failed to insert admin user")?;

        let admin_id = result.last_insert_rowid();

        // 6. Grant Admin Initial Permissions
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
        .await
        .context("Failed to grant admin bucket permissions")?;
        
        info!("Admin user setup successful. Granted 'read-write' for bucket '{}'.", DEFAULT_ADMIN_BUCKET);
    } else {
        info!("Users already exist. Skipping admin creation.");
    }

    Ok(())
}

pub async fn insert_user(
    pool: &SqlitePool,
    current_user_id: i64,
    new_username: &str,
    new_password: &str,
    new_user_role: &str,
    bucket_permissions: Option<Vec<(&str, &str)>>,
) -> Result<User> {
    // 1. Validate inputs
    if new_username.trim().is_empty() {
        return Err(anyhow::anyhow!("Username cannot be empty"));
    }
    if new_password.trim().is_empty() {
        return Err(anyhow::anyhow!("Password cannot be empty"));
    }
    if new_user_role != "admin" && new_user_role != "user" {
        return Err(anyhow::anyhow!("Role must be 'admin' or 'user'"));
    }

    // 2. Enforce admin-only user creation
    let current_user_role = get_user_role(pool, current_user_id).await?
        .context("Current user not found, cannot create new user")?;

    if current_user_role != "admin" {
        info!("Permission denied: Only 'admin' users can create new accounts. User role: {}", current_user_role);
        return Err(anyhow::anyhow!("Permission denied: Only 'admin' users can create new accounts"));
    }

    // 3. Hash password and insert user
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
    .await
    .context("Failed to insert new user")?;

    let new_user_id = result.last_insert_rowid();

    // Retrieve the newly created user
    let new_user = sqlx::query_as::<_, User>(
        "SELECT id, username, password_hash, role FROM users WHERE id = ?",
    )
    .bind(new_user_id)
    .fetch_one(pool)
    .await
    .context("Failed to fetch newly created user")?;

    // 4. Grant initial bucket permissions
    if let Some(permissions) = bucket_permissions {
        for (bucket_name, permission_level) in permissions {
            if permission_level != "read-only" && permission_level != "read-write" {
                return Err(anyhow::anyhow!("Invalid permission level: {}", permission_level));
            }
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
    
    info!("New user created successfully: {}", new_username);
    Ok(new_user)
}

pub async fn get_user_role(pool: &SqlitePool, user_id: i64) -> Result<Option<String>> {
    let row = sqlx::query("SELECT role FROM users WHERE id = ?")
        .bind(user_id)
        .fetch_optional(pool)
        .await
        .context("Failed to fetch user role")?;
    
    let role = row.map(|r| r.get("role"));
    Ok(role)
}

pub async fn verify_credentials(
    pool: &SqlitePool,
    username: &str,
    password: &str,
) -> Result<Option<User>> {
    info!("Verifying credentials for user: {}", username);

    let user_row = sqlx::query(
        r#"
        SELECT id, username, password_hash, role 
        FROM users 
        WHERE username = ?
        "#,
    )
    .bind(username)
    .fetch_optional(pool)
    .await
    .context("Failed to query user from database")?;

    let user_row = match user_row {
        Some(row) => row,
        None => {
            info!("User not found: {}", username);
            return Ok(None);
        }
    };

    let id: i64 = user_row.get("id");
    let username_str: String = user_row.get("username");
    let password_hash: String = user_row.get("password_hash");
    let role: String = user_row.get("role");

    let is_valid = verify_password(password, &password_hash)?;

    if is_valid {
        info!("User authenticated successfully: {}", username);
        Ok(Some(User {
            id,
            username: username_str,
            password_hash,
            role,
        }))
    } else {
        info!("Invalid password for user: {}", username);
        Ok(None)
    }
}

pub async fn get_user_by_username(pool: &SqlitePool, username: &str) -> Result<Option<User>> {
    let row = sqlx::query("SELECT id, username, password_hash, role FROM users WHERE username = ?")
        .bind(username)
        .fetch_optional(pool)
        .await
        .context("Failed to fetch user by username")?;
    
    let user = row.map(|r| User {
        id: r.get("id"),
        username: r.get("username"),
        password_hash: r.get("password_hash"),
        role: r.get("role"),
    });
    
    Ok(user)
}

pub async fn get_user_bucket_permissions(
    pool: &SqlitePool,
    user_id: i64,
) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT bucket_name, permission_level 
        FROM user_bucket_permissions 
        WHERE user_id = ?
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .context("Failed to fetch user bucket permissions")?;
    
    let permissions = rows.into_iter().map(|row| {
        (
            row.get("bucket_name"),
            row.get("permission_level"),
        )
    }).collect();
    
    Ok(permissions)
}

fn hash_password(password: &str) -> Result<String> {
    hash(password, DEFAULT_COST)
        .map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))
}

fn verify_password(password: &str, password_hash: &str) -> Result<bool> {
    verify(password, password_hash)
        .map_err(|e| anyhow::anyhow!("Failed to verify password: {}", e))
}