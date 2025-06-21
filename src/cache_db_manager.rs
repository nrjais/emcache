// Cache database manager module

use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Cache database manager for SQLite entity databases
#[derive(Clone)]
pub struct CacheDbManager {
    pools: Arc<RwLock<HashMap<String, SqlitePool>>>,
    base_dir: String,
}

/// Cache statistics for monitoring
#[derive(Debug, serde::Serialize)]
pub struct CacheStats {
    pub entity_name: String,
    pub document_count: i64,
    pub file_size_bytes: u64,
    pub last_update: Option<String>,
}

/// Disk usage statistics
#[derive(Debug, serde::Serialize)]
pub struct DiskUsageStats {
    pub total_size_bytes: u64,
    pub database_count: usize,
    pub base_directory: String,
    pub entity_sizes: HashMap<String, u64>,
}

impl CacheDbManager {
    /// Create a new CacheDbManager
    pub fn new() -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            base_dir: "dbs".to_string(),
        }
    }

    /// Create a new CacheDbManager with custom base directory
    pub fn with_base_dir(base_dir: String) -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            base_dir,
        }
    }

    /// Get or create a SQLite connection pool for an entity
    pub async fn get_or_create_pool(&self, entity_name: &str) -> Result<SqlitePool> {
        // Check if pool already exists
        {
            let pools = self.pools.read().await;
            if let Some(pool) = pools.get(entity_name) {
                return Ok(pool.clone());
            }
        }

        // Create new pool
        let pool = self.create_pool(entity_name).await?;

        // Store in the map
        {
            let mut pools = self.pools.write().await;
            pools.insert(entity_name.to_string(), pool.clone());
        }

        info!("Created SQLite connection pool for entity: {}", entity_name);
        Ok(pool)
    }

    /// Create a new SQLite connection pool for an entity
    async fn create_pool(&self, entity_name: &str) -> Result<SqlitePool> {
        let db_path = self.get_db_path(entity_name)?;

        // Ensure directory exists
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Create connection string
        let connection_string = format!("sqlite:{}", db_path.display());

        // Create pool with configuration
        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect(&connection_string)
            .await?;

        // Run migrations to set up the database
        self.run_migrations(&pool).await?;

        debug!("Created SQLite database at: {}", db_path.display());
        Ok(pool)
    }

    /// Get the database file path for an entity
    fn get_db_path(&self, entity_name: &str) -> Result<std::path::PathBuf> {
        let mut path = std::path::PathBuf::from(&self.base_dir);
        path.push(entity_name);
        path.push("cache.sqlite");
        Ok(path)
    }

    /// Run migrations on a SQLite database
    async fn run_migrations(&self, pool: &SqlitePool) -> Result<()> {
        // Create cache_data table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cache_data (
                doc_id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Create index for performance
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cache_data_version ON cache_data(version)")
            .execute(pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_cache_data_updated_at ON cache_data(updated_at)",
        )
        .execute(pool)
        .await?;

        // Create metadata table for this cache database
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cache_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(pool)
        .await?;

        debug!("SQLite migrations completed");
        Ok(())
    }

    /// Remove a connection pool and delete the database file
    pub async fn remove_entity_database(&self, entity_name: &str) -> Result<()> {
        info!("Removing cache database for entity: {}", entity_name);

        // Remove from pool cache
        {
            let mut pools = self.pools.write().await;
            if let Some(pool) = pools.remove(entity_name) {
                pool.close().await;
                debug!("Closed connection pool for entity: {}", entity_name);
            }
        }

        // Delete database files
        let entity_dir = Path::new(&self.base_dir).join(entity_name);
        if entity_dir.exists() {
            fs::remove_dir_all(&entity_dir).await?;
            info!("Deleted cache database directory: {}", entity_dir.display());
        }

        Ok(())
    }

    /// Check if a cache database exists for an entity
    pub async fn has_cache(&self, entity_name: &str) -> bool {
        let db_path = match self.get_db_path(entity_name) {
            Ok(path) => path,
            Err(_) => return false,
        };

        db_path.exists()
    }

    /// Get cache statistics for an entity
    pub async fn get_cache_stats(&self, entity_name: &str) -> Result<CacheStats> {
        let pool = self.get_or_create_pool(entity_name).await?;

        // Get document count
        let doc_count_row = sqlx::query("SELECT COUNT(*) as count FROM cache_data")
            .fetch_one(&pool)
            .await?;
        let document_count: i64 = doc_count_row.get("count");

        // Get database file size
        let db_path = self.get_db_path(entity_name)?;
        let file_size = if db_path.exists() {
            fs::metadata(&db_path).await?.len()
        } else {
            0
        };

        // Get last update time
        let last_update_row = sqlx::query("SELECT MAX(updated_at) as last_update FROM cache_data")
            .fetch_one(&pool)
            .await?;
        let last_update: Option<String> = last_update_row.get("last_update");

        Ok(CacheStats {
            entity_name: entity_name.to_string(),
            document_count,
            file_size_bytes: file_size,
            last_update,
        })
    }

    /// Get statistics for all cache databases
    pub async fn get_all_cache_stats(&self) -> Result<Vec<CacheStats>> {
        let pools = self.pools.read().await;
        let mut stats = Vec::new();

        for entity_name in pools.keys() {
            match self.get_cache_stats(entity_name).await {
                Ok(stat) => stats.push(stat),
                Err(e) => warn!("Failed to get stats for entity {}: {}", entity_name, e),
            }
        }

        Ok(stats)
    }

    /// Recreate a cache database (for corruption recovery)
    pub async fn recreate_cache(&self, entity_name: &str) -> Result<()> {
        info!("Recreating cache database for entity: {}", entity_name);

        // Remove existing database
        self.remove_entity_database(entity_name).await?;

        // Create new database
        self.get_or_create_pool(entity_name).await?;

        info!(
            "Successfully recreated cache database for entity: {}",
            entity_name
        );
        Ok(())
    }

    /// Shutdown all connection pools
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down cache database manager");

        let mut pools = self.pools.write().await;
        for (entity_name, pool) in pools.drain() {
            pool.close().await;
            debug!("Closed pool for entity: {}", entity_name);
        }

        info!("All cache database pools closed");
        Ok(())
    }

    /// Get disk usage statistics
    pub async fn get_disk_usage_stats(&self) -> Result<DiskUsageStats> {
        let base_path = Path::new(&self.base_dir);
        let mut total_size = 0u64;
        let mut database_count = 0usize;
        let mut entity_sizes = HashMap::new();

        if base_path.exists() {
            let mut entries = fs::read_dir(base_path).await?;

            while let Some(entry) = entries.next_entry().await? {
                if entry.file_type().await?.is_dir() {
                    let entity_name = entry.file_name().to_string_lossy().to_string();
                    let db_file = entry.path().join("cache.sqlite");

                    if db_file.exists() {
                        let size = fs::metadata(&db_file).await?.len();
                        total_size += size;
                        database_count += 1;
                        entity_sizes.insert(entity_name, size);
                    }
                }
            }
        }

        Ok(DiskUsageStats {
            total_size_bytes: total_size,
            database_count,
            base_directory: self.base_dir.clone(),
            entity_sizes,
        })
    }

    /// Cleanup unused entity databases
    pub async fn cleanup_unused_entity_databases(&self, active_entities: &[String]) -> Result<()> {
        info!("Cleaning up unused entity databases");

        let base_path = Path::new(&self.base_dir);
        if !base_path.exists() {
            return Ok(());
        }

        let mut entries = fs::read_dir(base_path).await?;
        let mut cleaned_count = 0;

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let entity_name = entry.file_name().to_string_lossy().to_string();

                if !active_entities.contains(&entity_name) {
                    match self.remove_entity_database(&entity_name).await {
                        Ok(_) => {
                            cleaned_count += 1;
                            info!("Removed unused database for entity: {}", entity_name);
                        }
                        Err(e) => warn!(
                            "Failed to remove database for entity {}: {}",
                            entity_name, e
                        ),
                    }
                }
            }
        }

        if cleaned_count > 0 {
            info!("Cleaned up {} unused entity databases", cleaned_count);
        }

        Ok(())
    }

    /// Cleanup old databases (older than specified days)
    pub async fn cleanup_old_databases(&self, older_than_days: u64) -> Result<u64> {
        info!("Cleaning up databases older than {} days", older_than_days);

        let cutoff = std::time::SystemTime::now()
            - std::time::Duration::from_secs(older_than_days * 24 * 60 * 60);
        let base_path = Path::new(&self.base_dir);
        let mut cleaned_count = 0u64;

        if !base_path.exists() {
            return Ok(0);
        }

        let mut entries = fs::read_dir(base_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let entity_name = entry.file_name().to_string_lossy().to_string();
                let db_file = entry.path().join("cache.sqlite");

                if db_file.exists() {
                    if let Ok(metadata) = fs::metadata(&db_file).await {
                        if let Ok(modified) = metadata.modified() {
                            if modified < cutoff {
                                match self.remove_entity_database(&entity_name).await {
                                    Ok(_) => {
                                        cleaned_count += 1;
                                        info!("Removed old database for entity: {}", entity_name);
                                    }
                                    Err(e) => warn!(
                                        "Failed to remove old database for entity {}: {}",
                                        entity_name, e
                                    ),
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(cleaned_count)
    }

    /// Cleanup unused connections
    pub async fn cleanup_unused_connections(&self) -> Result<()> {
        info!("Cleaning up unused cache database connections");

        // In a production system, you'd track last access times
        // and close pools that haven't been used for a while
        let pools = self.pools.read().await;
        let pool_count = pools.len();
        debug!("Currently managing {} cache database pools", pool_count);

        Ok(())
    }
}

impl Default for CacheDbManager {
    fn default() -> Self {
        Self::new()
    }
}
