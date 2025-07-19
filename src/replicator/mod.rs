use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{
    entity::EntityManager,
    executor::Task,
    oplog::OplogDatabase,
    replicator::{metadata::MetadataDb, sqlite::SqliteManager},
    types::Oplog,
};

pub mod cache;
mod mapper;
pub mod metadata;
mod migrator;
pub mod sqlite;

pub struct Replicator {
    sqlite_manager: Arc<SqliteManager>,
    database: OplogDatabase,
    entity_manager: Arc<EntityManager>,
    batch_size: i64,
    interval: Duration,
    meta: MetadataDb,
}

impl Replicator {
    pub fn new(
        entity_manager: Arc<EntityManager>,
        meta: MetadataDb,
        oplog_db: OplogDatabase,
        sqlite_manager: Arc<SqliteManager>,
        interval: Duration,
    ) -> Self {
        Self {
            meta,
            sqlite_manager,
            database: oplog_db,
            entity_manager,
            batch_size: 1000,
            interval,
        }
    }

    async fn replication_loop(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        info!("Starting cache replication loop");

        let mut entities_update = self.entity_manager.broadcast();
        let mut interval = tokio::time::interval(self.interval);
        let mut max_oplog_id = self.meta.max_oplog_id()?;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Cache replication loop received shutdown signal");
                    break;
                }
                _ = interval.tick() => {
                    let processed = self.poll_next(&mut max_oplog_id).await;
                    if processed {
                        interval.reset_immediately();
                    }
                }
                m = entities_update.recv() => {
                    if m.is_err() {
                        continue;
                    }
                    if let Err(e) = self.cleanup_orphaned_databases().await {
                        error!("Failed to cleanup orphaned databases: {}", e);
                    }
                }
            }
        }

        self.sqlite_manager.shutdown().await?;
        info!("Cache replication loop stopped");
        Ok(())
    }

    async fn cleanup_orphaned_databases(&self) -> anyhow::Result<()> {
        debug!("Cleaning up orphaned databases");

        let existing_entities = self.entity_manager.get_all_entities();
        let existing_entity_names: HashSet<String> = existing_entities.into_iter().map(|e| e.name).collect();

        self.sqlite_manager
            .cleanup_orphaned_databases(&existing_entity_names)
            .await?;

        Ok(())
    }

    async fn poll_next(&self, max_oplog_id: &mut i64) -> bool {
        let result = self.process_oplog_batch(*max_oplog_id).await;

        let max_processed_id = match result {
            Ok(max_processed_id) => max_processed_id,
            Err(e) => {
                error!("Failed to process oplog batch: {}", e);
                return false;
            }
        };

        let current_max_id = max_processed_id.max(*max_oplog_id);
        let count = current_max_id - *max_oplog_id;
        *max_oplog_id = current_max_id;

        let _ = self.update_max_oplog_id(current_max_id).await;
        debug!("Processed {} oplogs, last processed id: {}", count, current_max_id);

        count > 0
    }

    async fn process_oplog_batch(&self, max_oplog_id: i64) -> anyhow::Result<i64> {
        let oplogs = self.database.get_oplogs(max_oplog_id, self.batch_size).await?;

        if oplogs.is_empty() {
            return Ok(max_oplog_id);
        }

        debug!("Fetched {} oplogs to process", oplogs.len());

        let mut grouped_oplogs: HashMap<String, Vec<Oplog>> = HashMap::new();
        for oplog in oplogs {
            grouped_oplogs.entry(oplog.entity.clone()).or_default().push(oplog);
        }

        let mut max_processed_id = max_oplog_id;

        for (entity_name, oplogs) in grouped_oplogs {
            for oplog in &oplogs {
                max_processed_id = max_processed_id.max(oplog.id);
            }

            self.apply_entity_oplogs(&entity_name, oplogs).await?;
        }

        Ok(max_processed_id)
    }

    async fn apply_entity_oplogs(&self, entity_name: &str, oplogs: Vec<Oplog>) -> anyhow::Result<()> {
        let entity = self.entity_manager.get_entity_force_refresh(entity_name).await?;
        if let Some(entity) = entity {
            let cache = self.sqlite_manager.get_or_create_cache(&entity).await?;
            cache.apply_oplogs(oplogs).await?;
        } else {
            error!("Entity not found: {}, skipping oplogs", entity_name);
        }
        Ok(())
    }

    async fn update_max_oplog_id(&self, max_processed_id: i64) -> anyhow::Result<()> {
        self.meta.set_max_oplog_id(max_processed_id)
    }
}

impl Task for Replicator {
    fn name(&self) -> String {
        "replicator".to_string()
    }

    fn execute(&self, cancellation_token: CancellationToken) -> impl Future<Output = anyhow::Result<()>> + Send {
        self.replication_loop(cancellation_token)
    }
}
