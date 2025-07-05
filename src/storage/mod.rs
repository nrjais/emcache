use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::{PgPool, SqlitePool, postgres::PgPoolOptions, sqlite::SqlitePoolOptions};
use tracing::info;

use crate::config::AppConfig;

#[derive(Clone)]
pub struct PostgresClient {
    pub pool: PgPool,
}

impl PostgresClient {
    pub async fn new(config: AppConfig) -> Result<Self> {
        info!("Initializing database manager with auto migrations");

        let pool = PgPoolOptions::new()
            .max_connections(config.database.postgres.max_connections)
            .min_connections(config.database.postgres.min_connections)
            .acquire_timeout(Duration::from_millis(config.database.postgres.connection_timeout))
            .connect(&config.database.postgres.uri)
            .await?;

        info!("PostgreSQL connection pool created");

        Self::run_migrations(&pool).await?;

        info!("Database manager initialized successfully with migrations applied");
        Ok(Self { pool })
    }

    /// Run PostgreSQL migrations using SQLx auto migration
    async fn run_migrations(pool: &PgPool) -> Result<()> {
        info!("Running PostgreSQL auto migrations");
        sqlx::migrate!("./migrations").run(pool).await?;
        info!("PostgreSQL migrations completed successfully");
        Ok(())
    }

    /// Get PostgreSQL pool reference
    pub fn postgres(&self) -> &PgPool {
        &self.pool
    }
}

pub async fn metadata_sqlite(config: &AppConfig) -> anyhow::Result<SqlitePool> {
    let db = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(format!("sqlite:{}/metadata.db?mode=rwc", &config.cache.base_dir).as_str())
        .await
        .context("Failed to connect to metadata SQLite database")?;
    Ok(db)
}
