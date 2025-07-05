use anyhow::Result;
use tracing::debug;

use crate::storage::PostgresClient;
use crate::types::OplogEvent;

use super::database::OplogDatabase;

/// Oplog processor handles the business logic of processing oplogs
#[derive(Clone)]
pub struct OplogProcessor {
    db_ops: OplogDatabase,
}

impl OplogProcessor {
    /// Create a new oplog processor
    pub fn new(client: PostgresClient) -> Self {
        Self {
            db_ops: OplogDatabase::new(client),
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
}
