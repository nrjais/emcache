use std::path::{Path, PathBuf};

use dashmap::DashMap;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::fs;
use tracing::{debug, info};

use crate::{replicator::cache::LocalCache, types::Entity};

pub struct SqliteManager {
    dbs: DashMap<String, LocalCache>,
    base_dir: PathBuf,
}

impl SqliteManager {
    pub fn new(base_dir: &str) -> Self {
        Self {
            dbs: DashMap::new(),
            base_dir: Path::new(base_dir).join("cache"),
        }
    }

    pub async fn get_or_create_cache(&self, entity: &Entity) -> anyhow::Result<LocalCache> {
        if let Some(cache) = self.dbs.get(&entity.name) {
            return Ok(cache.clone());
        }

        let cache = self.create_cache(entity).await?;

        self.dbs.insert(entity.name.clone(), cache.clone());

        info!("Created SQLite connection pool for entity: {}", entity.name);
        Ok(cache)
    }

    async fn create_cache(&self, entity: &Entity) -> anyhow::Result<LocalCache> {
        let db_path = self.get_db_path(entity)?;

        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let connection_string = format!("sqlite:{}?mode=rwc", db_path.display());

        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect(&connection_string)
            .await?;

        debug!("Created SQLite database at: {}", db_path.display());
        let local_cache = LocalCache::new(pool, entity.clone());
        local_cache.init().await?;

        Ok(local_cache)
    }

    fn get_db_path(&self, entity: &Entity) -> anyhow::Result<std::path::PathBuf> {
        let mut path = self.base_dir.clone();
        path.push(entity.name.clone());
        path.push("cache.db");

        Ok(path)
    }

    pub async fn delete_database(&self, entity_name: &str) -> anyhow::Result<()> {
        info!("Removing cache database for entity: {}", entity_name);

        if let Some(pool) = self.dbs.remove(entity_name) {
            pool.1.close().await;
            debug!("Closed connection pool for entity: {}", entity_name);
        }

        let entity_dir = Path::new(&self.base_dir).join(entity_name);
        if entity_dir.exists() {
            fs::remove_dir_all(&entity_dir).await?;
            info!("Deleted cache database directory: {}", entity_dir.display());
        }

        Ok(())
    }

    pub async fn snapshot_to(&self, entity: &Entity, snapshot_path: &Path) -> anyhow::Result<()> {
        let cache = self.get_or_create_cache(entity).await?;
        cache.snapshot_to(snapshot_path).await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        for db in &self.dbs {
            debug!("Closing SQLite connection pool for entity: {}", db.key());
            db.value().close().await;
        }
        self.dbs.clear();

        Ok(())
    }
}
