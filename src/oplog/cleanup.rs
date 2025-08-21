use anyhow::Result;
use chrono::{Duration, Utc};
use serde_json::Value;
use std::sync::Arc;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::entity::EntityManager;
use crate::executor::Task;
use crate::storage::PostgresClient;
use crate::types::Operation;

pub struct OplogCleanupTask {
    postgres: PostgresClient,
    entity_manager: Arc<EntityManager>,
    cleanup_interval: std::time::Duration,
    retention_days: i64,
}

impl OplogCleanupTask {
    pub fn new(
        postgres: PostgresClient,
        entity_manager: Arc<EntityManager>,
        cleanup_interval: std::time::Duration,
        retention_days: i64,
    ) -> Self {
        Self {
            postgres,
            entity_manager,
            cleanup_interval,
            retention_days,
        }
    }

    async fn cleanup_old_oplogs(&self) -> Result<()> {
        let cutoff_time = Utc::now() - Duration::days(self.retention_days);
        info!("Starting oplog cleanup for logs older than {}", cutoff_time);

        let entities = self.entity_manager.get_all_entities();
        if entities.is_empty() {
            debug!("No entities found, skipping cleanup");
            return Ok(());
        }

        for entity in entities {
            let entity_name = &entity.name;
            debug!("Processing oplog cleanup for entity: {}", entity_name);
            let highest_id = sqlx::query_scalar!(
                "SELECT MAX(id) FROM oplog WHERE entity = $1 AND created_at < $2",
                entity_name,
                cutoff_time,
            )
            .fetch_one(self.postgres.postgres())
            .await?;

            let Some(highest_id) = highest_id else {
                info!("No oplogs found for entity: {}", entity_name);
                continue;
            };

            let mut tx = self.postgres.postgres().begin().await?;
            let deleted_count = sqlx::query!(
                "DELETE FROM oplog WHERE entity = $1 AND id <= $2",
                entity_name,
                highest_id
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();

            info!("Deleted {} old oplogs for entity: {}", deleted_count, entity_name);

            sqlx::query!(
                "UPDATE oplog SET operation = $2, data = $3 WHERE id = $1",
                highest_id,
                Operation::Tombstone.to_string(),
                &Value::Null,
            )
            .execute(&mut *tx)
            .await?;

            info!("Created tombstone oplog for entity: {}", entity_name);

            tx.commit().await?;
        }

        info!("Oplog cleanup completed successfully");
        Ok(())
    }
}

impl Task for OplogCleanupTask {
    fn name(&self) -> String {
        "oplog-cleanup".to_string()
    }

    async fn execute(&self, cancellation_token: CancellationToken) -> Result<()> {
        let mut interval = interval(self.cleanup_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.cleanup_old_oplogs().await {
                        error!("Failed to cleanup old oplogs: {}", e);
                    }
                }
                _ = cancellation_token.cancelled() => {
                    info!("Oplog cleanup task cancelled");
                    return Ok(());
                }
            }
        }
    }
}
