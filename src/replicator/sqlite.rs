use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use dashmap::DashMap;
use rusqlite::Connection;
use tokio::fs;
use tracing::{debug, info, warn};

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
            fs::create_dir_all(parent).await.context("Failed to create directory")?;
        }

        let connection = Connection::open(&db_path).context("Failed to open SQLite database")?;

        self.configure_sqlite_connection(&connection)?;

        let connection = Arc::new(Mutex::new(connection));

        debug!("Created SQLite database at: {}", db_path.display());
        let local_cache = LocalCache::new(connection, entity.clone());
        local_cache
            .init()
            .await
            .context("Failed to initialize SQLite database")?;

        Ok(local_cache)
    }

    fn configure_sqlite_connection(&self, connection: &Connection) -> anyhow::Result<()> {
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| anyhow::anyhow!("Failed to set journal mode: {}", e))?;
        connection
            .pragma_update(None, "wal_autocheckpoint", 8)
            .map_err(|e| anyhow::anyhow!("Failed to set WAL autocheckpoint: {}", e))?;
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| anyhow::anyhow!("Failed to set synchronous mode: {}", e))?;
        connection
            .pragma_update(None, "cache_size", 64)
            .map_err(|e| anyhow::anyhow!("Failed to set cache size: {}", e))?;
        connection
            .pragma_update(None, "temp_store", "MEMORY")
            .map_err(|e| anyhow::anyhow!("Failed to set temp store: {}", e))?;

        Ok(())
    }

    fn get_db_path(&self, entity: &Entity) -> anyhow::Result<std::path::PathBuf> {
        let mut path = self.base_dir.clone();
        path.push(entity.name.clone());
        path.push("cache.db");

        Ok(path)
    }

    pub async fn delete_database(&self, entity_name: &str) -> anyhow::Result<()> {
        info!("Removing cache database for entity: {}", entity_name);

        if self.dbs.remove(entity_name).is_some() {
            debug!("Closed connection for entity: {}", entity_name);
        }

        let entity_dir = Path::new(&self.base_dir).join(entity_name);
        fs::remove_dir_all(&entity_dir)
            .await
            .context("Failed to remove directory")?;
        info!("Deleted cache database directory: {}", entity_dir.display());

        Ok(())
    }

    pub async fn list_cached_entities(&self) -> anyhow::Result<Vec<String>> {
        let mut cached_entities = Vec::new();

        let mut entries = fs::read_dir(&self.base_dir)
            .await
            .context("Failed to read cache directory")?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(entity_name) = path.file_name().and_then(|n| n.to_str()) {
                    cached_entities.push(entity_name.to_string());
                }
            }
        }

        Ok(cached_entities)
    }

    pub async fn cleanup_orphaned_databases(&self, entities: &HashSet<String>) -> anyhow::Result<()> {
        let cached_entities = self.list_cached_entities().await?;

        for name in cached_entities {
            if entities.contains(&name) {
                continue;
            }

            match self.delete_database(&name).await {
                Ok(()) => {
                    info!("Cleaned up orphaned database for entity: {}", name);
                }
                Err(e) => {
                    warn!("Failed to cleanup orphaned database for entity {}: {}", name, e);
                }
            }
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.dbs.clear();

        Ok(())
    }
}
