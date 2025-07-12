pub mod snapshot_ref;

use std::fs::File;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use dashmap::DashMap;
use tokio::fs;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::Configs;
use crate::entity::EntityManager;
use crate::executor::Task;
use crate::replicator::cache::LocalCache;
use crate::replicator::sqlite::SqliteManager;
use crate::snapshot::snapshot_ref::{SNAPSHOT_DIR, SnapshotRef};
use crate::types::Entity;

struct Snapshot {
    guard: SnapshotRef,
    max_oplog_id: u64,
    cache: LocalCache,
}

pub struct SnapshotManager {
    entity_manager: Arc<EntityManager>,
    sqlite_manager: Arc<SqliteManager>,
    snapshots: DashMap<String, Snapshot>,
    check_interval: Duration,
    min_lag: u64,
    base_dir: String,
}

impl SnapshotManager {
    pub fn new(conf: &Configs, entity_manager: Arc<EntityManager>, sqlite_manager: Arc<SqliteManager>) -> Self {
        Self {
            base_dir: conf.cache.base_dir.to_string(),
            entity_manager,
            sqlite_manager,
            snapshots: DashMap::new(),
            check_interval: conf.snapshot.check_interval,
            min_lag: conf.snapshot.min_lag,
        }
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        info!("Initializing snapshot manager, creating snapshot directory");
        let snapshot_dir = format!("{}/{}", self.base_dir, SNAPSHOT_DIR);
        let _ = fs::remove_dir_all(&snapshot_dir).await;
        fs::create_dir_all(&snapshot_dir)
            .await
            .context("Failed to create snapshot directory")?;
        info!("Snapshot directory created at {}", snapshot_dir);
        Ok(())
    }

    pub async fn snapshot(&self, entity_name: &str) -> anyhow::Result<File> {
        let entity = self.entity_manager.get_entity_force_refresh(entity_name).await?;
        let Some(entity) = entity else {
            return Err(anyhow::anyhow!("Entity not found"));
        };

        if let Some(snapshot) = self.snapshots.get(&entity.name) {
            return snapshot.value().guard.open();
        }

        self.create_snapshot(&entity).await
    }

    async fn create_snapshot(&self, entity: &Entity) -> anyhow::Result<File> {
        let cache = self.sqlite_manager.get_or_create_cache(entity).await?;
        let (snapshot, max_oplog_id) = self.snapshot_for(entity, &cache).await?;

        let file = snapshot.open()?;
        self.snapshots.insert(
            entity.name.clone(),
            Snapshot {
                guard: snapshot,
                max_oplog_id,
                cache,
            },
        );

        Ok(file)
    }

    async fn snapshot_for(&self, entity: &Entity, cache: &LocalCache) -> anyhow::Result<(SnapshotRef, u64)> {
        let snapshot = SnapshotRef::new(&entity.name, &self.base_dir).await?;
        let max_oplog_id = cache.snapshot_to(snapshot.path())?;
        Ok((snapshot, max_oplog_id))
    }

    async fn recreate_stale_snapshots(&self) {
        self.snapshots
            .retain(|name, _| self.entity_manager.get_entity(name).is_some());

        for entity in self.entity_manager.get_all_entities() {
            let Some(mut snapshot) = self.snapshots.get_mut(&entity.name) else {
                let _ = self.create_snapshot(&entity).await.inspect_err(|e| {
                    error!("Failed to create snapshot for entity: {}: {}", entity.name, e);
                });
                continue;
            };

            let latest_offset = snapshot.cache.max_oplog_id();

            let lag = latest_offset.saturating_sub(snapshot.max_oplog_id);

            if lag > self.min_lag {
                let result = self.snapshot_for(&entity, &snapshot.cache).await;
                let Ok((snapshot_ref, max_oplog_id)) = result else {
                    error!("Failed to recreate stale snapshot for entity: {}", entity.name);
                    continue;
                };
                snapshot.max_oplog_id = max_oplog_id;
                snapshot.guard = snapshot_ref;
            }
        }
    }
}

impl Task for SnapshotManager {
    fn name(&self) -> String {
        "snapshot".to_string()
    }

    async fn execute(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut interval = interval(self.check_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    info!("Recreating stale snapshots");
                    self.recreate_stale_snapshots().await;
                }
                _ = cancellation_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }
}
