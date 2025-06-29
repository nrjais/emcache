use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

use crate::config::AppConfig;
use crate::entity::EntityManager;
use crate::storage::PostgresClient;
use crate::types::OplogEvent;
use futures::StreamExt;

use super::processor::OplogProcessor;
use futures_batch::ChunksTimeoutStreamExt;

pub struct OplogManager {
    config: AppConfig,
    processor: OplogProcessor,
}

impl OplogManager {
    pub async fn new(config: AppConfig, client: PostgresClient, entity_manager: Arc<EntityManager>) -> Result<Self> {
        let processor = OplogProcessor::new(client, entity_manager);

        Ok(Self { config, processor })
    }

    pub async fn run(&self, event_channel: mpsc::Receiver<OplogEvent>) -> Result<()> {
        let mut stream = ReceiverStream::new(event_channel).chunks_timeout(100, Duration::from_secs(1));
        while let Some(oplogs) = stream.next().await {
            self.processor.flush_batch(oplogs).await?;
        }

        Ok(())
    }
}
