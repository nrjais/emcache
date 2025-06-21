use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::database::DatabaseManager;
use crate::entity_manager::EntityManager;
use crate::types::{EntityStatus, Oplog};

use super::database::OplogDatabase;

/// Oplog processor handles the business logic of processing oplogs
#[derive(Clone)]
pub struct OplogProcessor {
    db_ops: OplogDatabase,
    entity_manager: Arc<EntityManager>,
}

impl OplogProcessor {
    /// Create a new oplog processor
    pub fn new(db_manager: Arc<DatabaseManager>, entity_manager: Arc<EntityManager>) -> Self {
        Self {
            db_ops: OplogDatabase::new(db_manager),
            entity_manager,
        }
    }

    /// Background task to process oplogs
    pub async fn process_oplogs(
        mut receiver: mpsc::Receiver<Oplog>,
        batch_buffer: Arc<Mutex<Vec<Oplog>>>,
        last_flush: Arc<Mutex<Instant>>,
        processor: OplogProcessor,
        ack_sender: Option<mpsc::Sender<i64>>,
        batch_size: usize,
        flush_interval: Duration,
    ) {
        info!("Oplog processing task started");

        loop {
            tokio::select! {
                // Receive new oplog entries
                oplog_opt = receiver.recv() => {
                    match oplog_opt {
                        Some(oplog) => {
                            debug!("Received oplog for entity: {}", oplog.entity);

                            // Add to batch buffer
                            {
                                let mut buffer = batch_buffer.lock().await;
                                buffer.push(oplog);

                                // Flush if batch size reached
                                if buffer.len() >= batch_size {
                                    if let Err(e) = processor.flush_batch(
                                        &mut buffer,
                                        &ack_sender
                                    ).await {
                                        error!("Failed to flush batch: {}", e);
                                    }
                                    *last_flush.lock().await = Instant::now();
                                }
                            }
                        }
                        None => {
                            info!("Oplog receiver channel closed");
                            break;
                        }
                    }
                }

                // Periodic flush based on time interval
                _ = sleep(flush_interval) => {
                    let should_flush = {
                        let last = last_flush.lock().await;
                        last.elapsed() >= flush_interval
                    };

                    if should_flush {
                        let mut buffer = batch_buffer.lock().await;
                        if !buffer.is_empty() {
                            debug!("Flushing {} oplogs due to time interval", buffer.len());
                            if let Err(e) = processor.flush_batch(
                                &mut buffer,
                                &ack_sender
                            ).await {
                                error!("Failed to flush batch: {}", e);
                            }
                            *last_flush.lock().await = Instant::now();
                        }
                    }
                }
            }
        }

        info!("Oplog processing task completed");
    }

    /// Flush batch of oplogs to database
    pub async fn flush_batch(
        &self,
        buffer: &mut Vec<Oplog>,
        ack_sender: &Option<mpsc::Sender<i64>>,
    ) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        debug!("Flushing batch of {} oplogs", buffer.len());

        // Group oplogs by entity for processing
        let mut grouped_oplogs: HashMap<String, Vec<Oplog>> = HashMap::new();
        for oplog in buffer.drain(..) {
            grouped_oplogs
                .entry(oplog.entity.clone())
                .or_default()
                .push(oplog);
        }

        // Process each entity group
        for (entity_name, entity_oplogs) in grouped_oplogs {
            if let Err(e) = self
                .process_entity_oplogs(&entity_name, entity_oplogs)
                .await
            {
                error!("Failed to process oplogs for entity {}: {}", entity_name, e);
                // Continue with other entities even if one fails
            }
        }

        // Send acknowledgment (placeholder - in real implementation, track individual oplogs)
        if let Some(sender) = ack_sender {
            if let Err(e) = sender.send(1).await {
                warn!("Failed to send acknowledgment: {}", e);
            }
        }

        Ok(())
    }

    /// Process oplogs for a specific entity
    async fn process_entity_oplogs(&self, entity_name: &str, oplogs: Vec<Oplog>) -> Result<()> {
        debug!(
            "Processing {} oplogs for entity: {}",
            oplogs.len(),
            entity_name
        );

        // Check entity status
        let entity_status = self.entity_manager.get_entity_status(entity_name).await?;

        match entity_status {
            Some(EntityStatus::Scanning) => {
                // Entity is being scanned - add to staging table
                self.db_ops.insert_to_staging(entity_name, oplogs).await?;
            }
            Some(EntityStatus::Live) => {
                // Entity is live - add to main oplog table
                self.db_ops.insert_to_oplog(entity_name, oplogs).await?;
            }
            Some(EntityStatus::Pending) => {
                // Entity is not ready - drop oplogs for now
                warn!("Dropping oplogs for pending entity: {}", entity_name);
            }
            Some(EntityStatus::Error) => {
                // Entity is in error state - drop oplogs
                warn!("Dropping oplogs for error entity: {}", entity_name);
            }
            None => {
                // Unknown entity - should not happen due to filtering in MongoDB listener
                warn!("Dropping oplogs for unknown entity: {}", entity_name);
            }
        }

        Ok(())
    }

    /// Move staging oplogs to main table after scanning completes
    pub async fn promote_staging_to_main(&self, entity_name: &str) -> Result<()> {
        self.db_ops.promote_staging_to_main(entity_name).await
    }

    /// Get staging oplog count for an entity
    pub async fn get_staging_count(&self, entity_name: &str) -> Result<i64> {
        self.db_ops.get_staging_count(entity_name).await
    }

    /// Get main oplog count for an entity
    pub async fn get_oplog_count(&self, entity_name: &str) -> Result<i64> {
        self.db_ops.get_oplog_count(entity_name).await
    }
}
