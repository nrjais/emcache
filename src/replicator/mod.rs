use std::{collections::HashMap, time::Duration};

use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::{
    entity::EntityManager,
    replicator::{database::OplogDatabase, meta::MetadataDb, sqlite::SqliteManager},
    storage::PostgresClient,
    types::Oplog,
};

mod cache;
mod database;
mod meta;
mod migrator;
mod sqlite;

pub struct Replicator {
    sqlite_manager: SqliteManager,
    database: OplogDatabase,
    entity_manager: EntityManager,
    batch_size: i64,
    interval: Duration,
    meta: MetadataDb,
}

impl Replicator {
    pub fn new(
        base_dir: &str,
        postgres_client: PostgresClient,
        entity_manager: EntityManager,
        meta: MetadataDb,
    ) -> Self {
        Self {
            meta,
            sqlite_manager: SqliteManager::new(base_dir),
            database: OplogDatabase::new(postgres_client),
            entity_manager,
            batch_size: 100,
            interval: Duration::from_secs(1),
        }
    }

    async fn replication_loop(&self) {
        info!("Starting cache replication loop");

        let mut interval = tokio::time::interval(self.interval);
        let mut last_processed_id = 0;

        loop {
            interval.tick().await;

            let result = self.process_oplog_batch(last_processed_id).await;

            match result {
                Ok(max_processed_id) => {
                    if max_processed_id == last_processed_id {
                        sleep(self.interval).await;
                    } else {
                        debug!("Processed {} oplogs", max_processed_id - last_processed_id);
                        last_processed_id = max_processed_id;
                        let _ = self.update_last_processed_id(max_processed_id).await;
                    }
                }
                Err(e) => {
                    error!("Failed to process oplog batch: {}", e);
                    sleep(self.interval).await;
                }
            }
        }
    }

    async fn process_oplog_batch(&self, last_processed_id: i64) -> anyhow::Result<i64> {
        let oplogs = self.database.get_oplogs(last_processed_id, self.batch_size).await?;

        if oplogs.is_empty() {
            return Ok(0);
        }

        debug!("Fetched {} oplogs to process", oplogs.len());

        let mut grouped_oplogs: HashMap<String, Vec<Oplog>> = HashMap::new();
        for oplog in oplogs {
            grouped_oplogs.entry(oplog.entity.clone()).or_default().push(oplog);
        }

        let mut max_processed_id = last_processed_id;

        for (entity_name, oplogs) in grouped_oplogs {
            for oplog in &oplogs {
                max_processed_id = max_processed_id.max(oplog.id);
            }

            self.apply_entity_oplogs(&entity_name, oplogs).await?;
        }

        Ok(max_processed_id)
    }

    async fn apply_entity_oplogs(&self, entity_name: &str, oplogs: Vec<Oplog>) -> anyhow::Result<()> {
        let entity = self.entity_manager.get_entity(entity_name).await?;
        if let Some(entity) = entity {
            let cache = self.sqlite_manager.get_or_create_cache(&entity).await?;
            cache.apply_oplogs(oplogs).await?;
        } else {
            error!("Entity not found: {}", entity_name);
        }
        Ok(())
    }

    async fn update_last_processed_id(&self, max_processed_id: i64) -> anyhow::Result<()> {
        self.meta.set_last_processed_id(max_processed_id).await
    }
}
