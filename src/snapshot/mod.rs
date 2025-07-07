pub mod snapshot;

use std::sync::Arc;

use dashmap::DashMap;

use crate::entity::EntityManager;
use crate::replicator::sqlite::SqliteManager;
use crate::snapshot::snapshot::{Snapshot, SnapshotRef};
use crate::types::Entity;

pub struct SnapshotManager {
    entity_manager: Arc<EntityManager>,
    sqlite_manager: Arc<SqliteManager>,
    snapshots: DashMap<String, Snapshot>,
}

impl SnapshotManager {
    pub fn new(entity_manager: Arc<EntityManager>, sqlite_manager: Arc<SqliteManager>) -> Self {
        Self {
            entity_manager,
            sqlite_manager,
            snapshots: DashMap::new(),
        }
    }

    pub async fn snapshot(&self, entity: &Entity) -> anyhow::Result<SnapshotRef> {
        if let Some(snapshot) = self.snapshots.get(&entity.name) {
            return snapshot.clone();
        }

        let snapshot_ref = self.create_snapshot(entity).await?;
        Ok(snapshot_ref)
    }

    async fn create_snapshot(&self, entity: &Entity) -> anyhow::Result<SnapshotRef> {
        let snapshot = Snapshot::new(&entity.name)?;

        self.sqlite_manager.snapshot_to(entity, snapshot.path()).await?;

        let snapshot_ref = snapshot.clone()?;

        self.snapshots.insert(entity.name.clone(), snapshot);

        Ok(snapshot_ref)
    }
}
