use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use tracing::{debug, info};

use crate::config::AppConfig;
use crate::database::DatabaseManager;
use crate::entity_manager::EntityManager;
use crate::job_server::Job;
use crate::types::Oplog;

use super::processor::OplogProcessor;

/// Oplog manager for processing MongoDB change events
pub struct OplogManager {
    name: String,
    config: AppConfig,
    db_manager: Arc<DatabaseManager>,
    entity_manager: Arc<EntityManager>,
    oplog_receiver: Option<mpsc::Receiver<Oplog>>,
    ack_sender: Option<mpsc::Sender<i64>>,
    batch_buffer: Arc<Mutex<Vec<Oplog>>>,
    last_flush: Arc<Mutex<Instant>>,
    processor: OplogProcessor,
}

impl OplogManager {
    /// Create a new Oplog Manager
    pub async fn new(
        config: AppConfig,
        db_manager: Arc<DatabaseManager>,
        entity_manager: Arc<EntityManager>,
    ) -> Result<Self> {
        info!("Initializing Oplog Manager");

        let processor = OplogProcessor::new(Arc::clone(&db_manager), Arc::clone(&entity_manager));

        Ok(Self {
            name: "oplog_manager".to_string(),
            config,
            db_manager,
            entity_manager,
            oplog_receiver: None,
            ack_sender: None,
            batch_buffer: Arc::new(Mutex::new(Vec::new())),
            last_flush: Arc::new(Mutex::new(Instant::now())),
            processor,
        })
    }

    /// Set up channels for oplog communication
    pub fn setup_channels(
        &mut self,
        oplog_receiver: mpsc::Receiver<Oplog>,
        ack_sender: mpsc::Sender<i64>,
    ) {
        self.oplog_receiver = Some(oplog_receiver);
        self.ack_sender = Some(ack_sender);
    }

    /// Start processing oplogs
    pub async fn start_processing(&mut self) -> Result<()> {
        info!("Starting oplog processing");

        let receiver = self.oplog_receiver.take();
        if receiver.is_none() {
            return Err(anyhow::anyhow!("Oplog receiver not configured"));
        }

        let receiver = receiver.unwrap();
        let batch_buffer = Arc::clone(&self.batch_buffer);
        let last_flush = Arc::clone(&self.last_flush);
        let processor = self.processor.clone();
        let ack_sender = self.ack_sender.clone();
        let batch_size = self.config.batch.oplog_batch_size;

        // Use a reasonable flush interval
        let flush_interval =
            tokio::time::Duration::from_millis(self.config.batch.oplog_batch_size as u64 * 10);

        // Spawn background task to handle oplog processing
        tokio::spawn(async move {
            OplogProcessor::process_oplogs(
                receiver,
                batch_buffer,
                last_flush,
                processor,
                ack_sender,
                batch_size,
                flush_interval,
            )
            .await
        });

        info!("Oplog processing started");
        Ok(())
    }

    /// Move staging oplogs to main table after scanning completes
    pub async fn promote_staging_to_main(&self, entity_name: &str) -> Result<()> {
        self.processor.promote_staging_to_main(entity_name).await
    }

    /// Get staging oplog count for an entity
    pub async fn get_staging_count(&self, entity_name: &str) -> Result<i64> {
        self.processor.get_staging_count(entity_name).await
    }

    /// Get main oplog count for an entity
    pub async fn get_oplog_count(&self, entity_name: &str) -> Result<i64> {
        self.processor.get_oplog_count(entity_name).await
    }
}

#[async_trait::async_trait]
impl Job for OplogManager {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self) -> Result<()> {
        debug!("Oplog manager job execution");
        // In a real implementation, this could handle:
        // - Cleanup of old oplogs
        // - Statistics collection
        // - Health monitoring
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down oplog manager");

        // Flush any remaining oplogs in buffer
        let mut buffer = self.batch_buffer.lock().await;
        if !buffer.is_empty() {
            info!("Flushing {} remaining oplogs on shutdown", buffer.len());
            if let Err(e) = self
                .processor
                .flush_batch(&mut buffer, &self.ack_sender)
                .await
            {
                tracing::error!("Failed to flush remaining oplogs: {}", e);
            }
        }

        Ok(())
    }
}
