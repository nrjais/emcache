use anyhow::Result;
use std::sync::Arc;
use tracing::debug;

use crate::entity::EntityManager;
use crate::storage::PostgresClient;
use crate::types::OplogEvent;

use super::database::OplogDatabase;

/// Oplog processor handles the business logic of processing oplogs
#[derive(Clone)]
pub struct OplogProcessor {
    db_ops: OplogDatabase,
    entity_manager: Arc<EntityManager>,
}

impl OplogProcessor {
    /// Create a new oplog processor
    pub fn new(client: PostgresClient, entity_manager: Arc<EntityManager>) -> Self {
        Self {
            db_ops: OplogDatabase::new(client),
            entity_manager,
        }
    }

    /// Flush batch of oplogs to database
    pub async fn flush_batch(&self, oplogs: Vec<OplogEvent>) -> Result<()> {
        if oplogs.is_empty() {
            return Ok(());
        }

        debug!("Flushing batch of {} oplogs", oplogs.len());

        let oplogs = oplogs.into_iter().map(|oplog| oplog.oplog).collect::<Vec<_>>();

        self.db_ops.insert_to_oplog(oplogs).await?;

        Ok(())
    }

    /// Process oplogs for a specific entity
    async fn process_entity_oplogs(&self, entity_name: &str, oplogs: Vec<OplogEvent>) -> Result<()> {
        debug!("Processing {} oplogs for entity: {}", oplogs.len(), entity_name);

        Ok(())
    }
}
