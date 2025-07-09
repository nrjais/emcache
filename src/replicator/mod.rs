use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{
    entity::EntityManager,
    executor::Task,
    replicator::{database::OplogDatabase, metadata::MetadataDb, sqlite::SqliteManager},
    storage::PostgresClient,
    types::Oplog,
};

pub mod cache;
mod database;
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
        postgres_client: PostgresClient,
        entity_manager: Arc<EntityManager>,
        meta: MetadataDb,
        sqlite_manager: Arc<SqliteManager>,
        interval: Duration,
    ) -> Self {
        Self {
            meta,
            sqlite_manager,
            database: OplogDatabase::new(postgres_client),
            entity_manager,
            batch_size: 100,
            interval,
        }
    }

    async fn replication_loop(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        info!("Starting cache replication loop");

        let mut interval = tokio::time::interval(self.interval);
        let mut last_processed_id = self.meta.get_last_processed_id()?;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Cache replication loop received shutdown signal");
                    break;
                }
                _ = interval.tick() => {
                    let processed = self.poll_next(&mut last_processed_id).await;
                    if processed {
                        interval.reset_immediately();
                    }
                }
            }
        }

        self.sqlite_manager.shutdown().await?;
        info!("Cache replication loop stopped");
        Ok(())
    }

    async fn poll_next(&self, last_processed_id: &mut i64) -> bool {
        let result = self.process_oplog_batch(*last_processed_id).await;

        let max_processed_id = match result {
            Ok(max_processed_id) => max_processed_id,
            Err(e) => {
                error!("Failed to process oplog batch: {}", e);
                return false;
            }
        };

        let max_processed_id = max_processed_id.max(*last_processed_id);
        let count = max_processed_id - *last_processed_id;
        *last_processed_id = max_processed_id;

        let _ = self.update_last_processed_id(max_processed_id).await;
        info!("Processed {} oplogs, last processed id: {}", count, max_processed_id);

        count > 0
    }

    async fn process_oplog_batch(&self, last_processed_id: i64) -> anyhow::Result<i64> {
        let oplogs = self.database.get_oplogs(last_processed_id, self.batch_size).await?;

        if oplogs.is_empty() {
            return Ok(last_processed_id);
        }

        debug!("Fetched {} oplogs to process", oplogs.len());

        let mut grouped_oplogs: HashMap<String, Vec<Oplog>> = HashMap::new();
        for oplog in oplogs {
            grouped_oplogs.entry(oplog.entity.clone()).or_default().push(oplog);
        }

        let mut max_processed_id = last_processed_id;

        for (entity_name, oplogs) in grouped_oplogs {
            for oplog in &oplogs {
                max_processed_id = max_processed_id.max(oplog.id as i64);
            }

            self.apply_entity_oplogs(&entity_name, oplogs).await?;
        }

        Ok(max_processed_id)
    }

    async fn apply_entity_oplogs(&self, entity_name: &str, oplogs: Vec<Oplog>) -> anyhow::Result<()> {
        let entity = self.entity_manager.get_entity(entity_name);
        if let Some(entity) = entity {
            let cache = self.sqlite_manager.get_or_create_cache(&entity).await?;
            cache.apply_oplogs(oplogs).await?;
        } else {
            error!("Entity not found: {}", entity_name);
        }
        Ok(())
    }

    async fn update_last_processed_id(&self, max_processed_id: i64) -> anyhow::Result<()> {
        self.meta.set_last_processed_id(max_processed_id)
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
