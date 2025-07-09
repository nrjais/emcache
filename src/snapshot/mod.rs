pub mod snapshot_ref;

use std::fs::File;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::fs;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::Configs;
use crate::entity::EntityManager;
use crate::executor::Task;
use crate::replicator::sqlite::SqliteManager;
use crate::snapshot::snapshot_ref::{SNAPSHOT_DIR, SnapshotRef};
use crate::types::Entity;

pub struct SnapshotManager {
    entity_manager: Arc<EntityManager>,
    sqlite_manager: Arc<SqliteManager>,
    snapshots: DashMap<String, SnapshotRef>,
    cleanup_interval: Duration,
    staleness_duration: Duration,
    base_dir: String,
}

impl SnapshotManager {
    pub fn new(conf: &Configs, entity_manager: Arc<EntityManager>, sqlite_manager: Arc<SqliteManager>) -> Self {
        Self {
            base_dir: conf.cache.base_dir.to_string(),
            entity_manager,
            sqlite_manager,
            snapshots: DashMap::new(),
            cleanup_interval: conf.snapshot.cleanup_interval,
            staleness_duration: conf.snapshot.staleness_duration,
        }
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        let snapshot_dir = format!("{}/{}", self.base_dir, SNAPSHOT_DIR);

        fs::create_dir_all(&snapshot_dir).await?;

        let mut entries = fs::read_dir(&snapshot_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let entity_dir = entry.path();
            if !entry.file_type().await?.is_dir() {
                continue;
            }

            let Some(entity) = entity_dir
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| self.entity_manager.get_entity(name))
            else {
                debug!("Found snapshot directory for unknown entity: {:?}", entity_dir);
                if let Err(e) = fs::remove_dir_all(&entity_dir).await {
                    warn!("Failed to remove orphaned snapshot directory {:?}: {}", entity_dir, e);
                }
                continue;
            };

            let Ok(mut entity_entries) = fs::read_dir(&entity_dir).await else {
                warn!("Failed to read snapshot directory {:?}", entity_dir);
                continue;
            };

            while let Some(file_entry) = entity_entries.next_entry().await? {
                let file_path = file_entry.path();

                let is_snapshot_file = file_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.ends_with("-snap.db"))
                    .unwrap_or(false);
                let is_file = file_entry.file_type().await?.is_file();

                if is_file && is_snapshot_file {
                    info!("Loaded existing snapshot for entity: {}", entity.name);
                    self.snapshots
                        .insert(entity.name.clone(), SnapshotRef::from_existing(file_path));
                    break;
                }
            }
        }
        info!("Loaded {} existing snapshots", self.snapshots.len());
        Ok(())
    }

    pub async fn snapshot(&self, entity_name: &str) -> anyhow::Result<File> {
        let entity = self
            .entity_manager
            .get_entity(entity_name)
            .ok_or(anyhow::anyhow!("Entity not found"))?;

        if let Some(snapshot) = self.snapshots.get(&entity.name) {
            return snapshot.value().open();
        }

        self.create_snapshot(&entity).await
    }

    async fn create_snapshot(&self, entity: &Entity) -> anyhow::Result<File> {
        let snapshot = SnapshotRef::new(&entity.name, &self.base_dir)?;
        self.sqlite_manager.snapshot_to(entity, snapshot.path()).await?;

        let file = snapshot.open()?;
        self.snapshots.insert(entity.name.clone(), snapshot);

        Ok(file)
    }

    fn recreate_stale_snapshots(&self) {
        self.snapshots
            .retain(|_, snapshot| !snapshot.is_stale(self.staleness_duration));
    }
}

impl Task for SnapshotManager {
    fn name(&self) -> String {
        "snapshot".to_string()
    }

    async fn execute(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut interval = interval(self.cleanup_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.recreate_stale_snapshots();
                }
                _ = cancellation_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }
}
