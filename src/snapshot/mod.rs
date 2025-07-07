pub mod snapshot_ref;

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use crate::entity::EntityManager;
use crate::executor::Task;
use crate::replicator::sqlite::SqliteManager;
use crate::snapshot::snapshot_ref::{Snapshot, SnapshotRef};
use crate::types::Entity;

pub struct SnapshotManager {
    entity_manager: Arc<EntityManager>,
    sqlite_manager: Arc<SqliteManager>,
    snapshots: DashMap<String, (Snapshot, i64)>,
}

impl SnapshotManager {
    pub fn new(entity_manager: Arc<EntityManager>, sqlite_manager: Arc<SqliteManager>) -> Self {
        Self {
            entity_manager,
            sqlite_manager,
            snapshots: DashMap::new(),
        }
    }

    pub async fn snapshot(&self, entity_name: &str) -> anyhow::Result<SnapshotRef> {
        let entity = self
            .entity_manager
            .get_entity(entity_name)
            .ok_or(anyhow::anyhow!("Entity not found"))?;

        if let Some(snapshot) = self.snapshots.get(&entity.name) {
            return snapshot.value().0.clone();
        }

        let snapshot_ref = self.create_snapshot(&entity).await?;
        Ok(snapshot_ref)
    }

    async fn create_snapshot(&self, entity: &Entity) -> anyhow::Result<SnapshotRef> {
        let snapshot = Snapshot::new(&entity.name)?;

        let last_processed_id = self.sqlite_manager.snapshot_to(entity, snapshot.path()).await?;

        let snapshot_ref = snapshot.clone()?;

        self.snapshots
            .insert(entity.name.clone(), (snapshot, last_processed_id));

        Ok(snapshot_ref)
    }

    fn remove_stale_snapshots(&self) {
        self.snapshots
            .retain(|_, (snapshot, _)| !snapshot.is_stale(Duration::from_secs(10)));
    }
}

impl Task for SnapshotManager {
    fn name(&self) -> String {
        "snapshot".to_string()
    }

    async fn execute(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut interval = interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.remove_stale_snapshots();
                }
                _ = cancellation_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }
}
