use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use rusqlite::Connection;
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

        info!("Created SQLite connection for entity: {}", entity.name);
        Ok(cache)
    }

    async fn create_cache(&self, entity: &Entity) -> anyhow::Result<LocalCache> {
        let db_path = self.get_db_path(entity)?;

        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let connection = Connection::open(&db_path)?;

        connection.execute("PRAGMA journal_mode = WAL", [])?;
        connection.execute("PRAGMA synchronous = NORMAL", [])?;
        connection.execute("PRAGMA cache_size = 1000", [])?;
        connection.execute("PRAGMA temp_store = MEMORY", [])?;

        let connection = Arc::new(Mutex::new(connection));

        debug!("Created SQLite database at: {}", db_path.display());
        let local_cache = LocalCache::new(connection, entity.clone());
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

        if let Some(_) = self.dbs.remove(entity_name) {
            debug!("Closed connection for entity: {}", entity_name);
        }

        let entity_dir = Path::new(&self.base_dir).join(entity_name);
        if entity_dir.exists() {
            fs::remove_dir_all(&entity_dir).await?;
            info!("Deleted cache database directory: {}", entity_dir.display());
        }

        Ok(())
    }

    pub async fn snapshot_to(&self, entity: &Entity, snapshot_path: &Path) -> anyhow::Result<i64> {
        let cache = self.get_or_create_cache(entity).await?;
        cache.snapshot_to(snapshot_path).await
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.dbs.clear();

        Ok(())
    }
}
