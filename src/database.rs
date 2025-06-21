use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, sqlite::SqlitePoolOptions, PgPool, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::config::AppConfig;

/// Database manager for PostgreSQL and SQLite connections with auto migration support
#[derive(Clone)]
pub struct DatabaseManager {
    pub postgres_pool: PgPool,
    sqlite_pools: Arc<RwLock<HashMap<String, SqlitePool>>>,
    config: AppConfig,
}

impl DatabaseManager {
    /// Create a new database manager with auto migration support
    pub async fn new(config: AppConfig) -> Result<Self> {
        info!("Initializing database manager with auto migrations");

        // Create PostgreSQL connection pool
        let postgres_pool = PgPoolOptions::new()
            .max_connections(config.connection_pool.postgres_max_connections)
            .min_connections(config.connection_pool.postgres_min_connections)
            .acquire_timeout(config.connection_timeout_duration())
            .idle_timeout(Some(config.idle_timeout_duration()))
            .connect(&config.database.postgres_url)
            .await?;

        info!("PostgreSQL connection pool created");

        // Run auto migrations using embedded migration files
        Self::run_migrations(&postgres_pool).await?;

        let manager = Self {
            postgres_pool,
            sqlite_pools: Arc::new(RwLock::new(HashMap::new())),
            config,
        };

        info!("Database manager initialized successfully with migrations applied");
        Ok(manager)
    }

    /// Run PostgreSQL migrations using SQLx auto migration
    async fn run_migrations(pool: &PgPool) -> Result<()> {
        info!("Running PostgreSQL auto migrations");

        // Use SQLx embedded migrations from the migrations directory
        sqlx::migrate!("./migrations").run(pool).await?;

        info!("PostgreSQL migrations completed successfully");
        Ok(())
    }

    /// Get PostgreSQL pool reference
    pub fn postgres(&self) -> &PgPool {
        &self.postgres_pool
    }

    /// Get or create SQLite pool for an entity
    pub async fn get_or_create_sqlite_pool(&self, entity_name: &str) -> Result<SqlitePool> {
        // Check if pool already exists
        {
            let pools = self.sqlite_pools.read().await;
            if let Some(pool) = pools.get(entity_name) {
                return Ok(pool.clone());
            }
        }

        // Create new pool
        let db_path = self.get_sqlite_path(entity_name)?;
        let pool = self.create_sqlite_pool(&db_path).await?;

        // Store in cache
        {
            let mut pools = self.sqlite_pools.write().await;
            pools.insert(entity_name.to_string(), pool.clone());
        }

        debug!("Created SQLite pool for entity: {}", entity_name);
        Ok(pool)
    }

    /// Create SQLite connection pool with schema initialization
    async fn create_sqlite_pool(&self, db_path: &Path) -> Result<SqlitePool> {
        // Ensure directory exists
        if let Some(parent) = db_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let db_url = format!("sqlite:{}", db_path.display());
        let pool = SqlitePoolOptions::new()
            .max_connections(self.config.connection_pool.sqlite_max_connections)
            .acquire_timeout(self.config.connection_timeout_duration())
            .connect(&db_url)
            .await?;

        // Initialize SQLite schema for cache databases
        self.init_sqlite_schema(&pool).await?;

        Ok(pool)
    }

    /// Initialize SQLite schema for cache databases
    async fn init_sqlite_schema(&self, pool: &SqlitePool) -> Result<()> {
        // Create cache table with optimized schema
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cache_data (
                doc_id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Create metadata table for cache database
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cache_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Create indexes for performance
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_cache_data_updated_at ON cache_data(updated_at)",
        )
        .execute(pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cache_data_version ON cache_data(version)")
            .execute(pool)
            .await?;

        Ok(())
    }

    /// Get SQLite database path for an entity
    fn get_sqlite_path(&self, entity_name: &str) -> Result<std::path::PathBuf> {
        let base_path = Path::new(&self.config.database.sqlite_base_path);
        let db_path = base_path.join(entity_name).join("cache.sqlite");
        Ok(db_path)
    }

    /// Remove SQLite pool and delete database files for an entity
    pub async fn remove_entity_database(&self, entity_name: &str) -> Result<()> {
        info!("Removing database for entity: {}", entity_name);

        // Remove from cache
        {
            let mut pools = self.sqlite_pools.write().await;
            if let Some(pool) = pools.remove(entity_name) {
                pool.close().await;
            }
        }

        // Delete database files
        let entity_dir = Path::new(&self.config.database.sqlite_base_path).join(entity_name);
        if entity_dir.exists() {
            tokio::fs::remove_dir_all(&entity_dir).await?;
            info!("Deleted database directory: {}", entity_dir.display());
        }

        Ok(())
    }

    /// Get metadata from PostgreSQL
    pub async fn get_metadata(&self, key: &str) -> Result<Option<String>> {
        let result = sqlx::query_scalar("SELECT value FROM metadata WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.postgres_pool)
            .await?;

        Ok(result)
    }

    /// Set metadata in PostgreSQL
    pub async fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO metadata (key, value) VALUES ($1, $2)
             ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()",
        )
        .bind(key)
        .bind(value)
        .execute(&self.postgres_pool)
        .await?;

        Ok(())
    }

    /// Clean up unused SQLite connections
    pub async fn cleanup_unused_connections(&self) -> Result<()> {
        info!("Cleaning up unused SQLite connections");

        // In a production system, you'd track last access times
        // and close pools that haven't been used for a while
        let pools = self.sqlite_pools.read().await;
        let pool_count = pools.len();
        debug!("Currently managing {} SQLite pools", pool_count);

        Ok(())
    }

    /// Health check for database connections
    pub async fn health_check(&self) -> Result<()> {
        // Check PostgreSQL connection
        sqlx::query("SELECT 1").execute(&self.postgres_pool).await?;

        debug!("Database health check passed");
        Ok(())
    }
}
