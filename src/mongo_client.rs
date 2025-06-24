use anyhow::Result;
use futures::TryStreamExt;
use mongodb::{
    Client, Collection, Database,
    bson::{Bson, Document, doc},
    change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken},
    options::{ChangeStreamOptions, FullDocumentType},
};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::types::{Entity, Operation, Oplog};
use crate::utils::RetryExecutor;

/// MongoDB client with change stream processing capabilities
#[derive(Clone)]
pub struct MongoClient {
    client: Option<Client>,
    database_name: String,
    connection_state: Arc<RwLock<ConnectionState>>,
    retry_executor: RetryExecutor,
}

/// Connection state tracking
#[derive(Debug, Default)]
struct ConnectionState {
    is_connected: bool,
    connection_failures: u64,
    active_streams: u64,
}

/// Change stream handle for managing active streams
pub struct ChangeStreamHandle {
    entity_name: String,
    stop_sender: mpsc::Sender<()>,
}

impl MongoClient {
    /// Create a new MongoDB client
    pub async fn new(config: &AppConfig) -> Result<Self> {
        info!("Initializing MongoDB client with change stream support");

        let database_name = Self::extract_database_name(&config.database.mongodb_url)?;
        let retry_executor = RetryExecutor::new();

        let client = Self {
            client: None,
            database_name,
            connection_state: Arc::new(RwLock::new(ConnectionState::default())),
            retry_executor,
        };

        info!(
            "MongoDB client initialized for database: {}",
            client.database_name
        );
        Ok(client)
    }

    /// Extract database name from MongoDB URL
    fn extract_database_name(mongodb_url: &str) -> Result<String> {
        if let Some(db_part) = mongodb_url.split('/').next_back() {
            if let Some(db_name) = db_part.split('?').next() {
                if !db_name.is_empty() {
                    return Ok(db_name.to_string());
                }
            }
        }
        Ok("emcachers".to_string()) // Default database name
    }

    /// Connect to MongoDB with retry logic
    pub async fn connect(&mut self, mongodb_url: &str) -> Result<()> {
        info!("Connecting to MongoDB with retry support");

        let connection_result = self
            .retry_executor
            .execute(|| async {
                let client = Client::with_uri_str(mongodb_url).await?;

                // Test connection with ping
                client
                    .database(&self.database_name)
                    .run_command(doc! {"ping": 1})
                    .await?;

                Ok::<Client, anyhow::Error>(client)
            })
            .await;

        match connection_result {
            Ok(client) => {
                self.client = Some(client);

                // Update connection state
                {
                    let mut state = self.connection_state.write().await;
                    state.is_connected = true;
                    state.connection_failures = 0;
                }

                info!(
                    "Successfully connected to MongoDB database: {}",
                    self.database_name
                );
                Ok(())
            }
            Err(e) => {
                self.handle_connection_failure().await;
                Err(e)
            }
        }
    }

    /// Get database reference
    pub fn database(&self) -> Option<Database> {
        self.client
            .as_ref()
            .map(|client| client.database(&self.database_name))
    }

    /// Get collection for an entity
    pub fn get_collection(&self, entity: &Entity) -> Option<Collection<Document>> {
        self.database()
            .map(|db| db.collection(&entity.shape.source_name))
    }

    /// Start change stream for an entity
    pub async fn start_change_stream(
        &self,
        entity: &Entity,
        oplog_sender: mpsc::Sender<Oplog>,
        resume_token: Option<String>,
    ) -> Result<ChangeStreamHandle> {
        info!("Starting change stream for entity: {}", entity.name);

        let collection = self
            .get_collection(entity)
            .ok_or_else(|| anyhow::anyhow!("MongoDB not connected"))?;

        let (stop_sender, mut stop_receiver) = mpsc::channel::<()>(1);

        // Configure change stream options
        let mut options = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .build();

        // Set resume token if provided
        if let Some(token) = resume_token {
            if let Ok(token_doc) = serde_json::from_str::<Document>(&token) {
                if let Ok(resume_token) = mongodb::bson::from_document::<ResumeToken>(token_doc) {
                    options.resume_after = Some(resume_token);
                    debug!("Using resume token for entity: {}", entity.name);
                }
            }
        }

        // Clone necessary data for the task
        let entity_clone = entity.clone();
        let entity_name = entity.name.clone();
        let connection_state = Arc::clone(&self.connection_state);

        // Start change stream monitoring task
        tokio::spawn(async move {
            if let Err(e) = Self::monitor_change_stream(
                collection,
                entity_clone,
                oplog_sender,
                options,
                &mut stop_receiver,
                connection_state,
            )
            .await
            {
                error!(
                    "Change stream monitoring failed for entity {}: {}",
                    entity_name, e
                );
            }
        });

        // Update active streams count
        {
            let mut state = self.connection_state.write().await;
            state.active_streams += 1;
        }

        Ok(ChangeStreamHandle {
            entity_name: entity.name.clone(),
            stop_sender,
        })
    }

    /// Monitor change stream for a specific entity
    async fn monitor_change_stream(
        collection: Collection<Document>,
        entity: Entity,
        oplog_sender: mpsc::Sender<Oplog>,
        options: ChangeStreamOptions,
        stop_receiver: &mut mpsc::Receiver<()>,
        connection_state: Arc<RwLock<ConnectionState>>,
    ) -> Result<()> {
        info!(
            "Starting change stream monitoring for entity: {}",
            entity.name
        );

        let mut change_stream = collection.watch().with_options(options.clone()).await?;

        loop {
            tokio::select! {
                // Check for stop signal
                _ = stop_receiver.recv() => {
                    info!("Stopping change stream for entity: {}", entity.name);
                    break;
                }

                // Process change stream events
                change_result = change_stream.try_next() => {
                    match change_result {
                        Ok(Some(change_event)) => {
                            if let Err(e) = Self::process_change_event(
                                &entity,
                                change_event,
                                &oplog_sender,
                            ).await {
                                error!("Failed to process change event for entity {}: {}", entity.name, e);
                            }
                        }
                        Ok(None) => {
                            warn!("Change stream ended for entity: {}", entity.name);
                            break;
                        }
                        Err(e) => {
                            error!("Change stream error for entity {}: {}", entity.name, e);

                            // Attempt to reconnect after a delay
                            sleep(Duration::from_secs(5)).await;

                            // Try to resume the change stream
                            match collection.watch().with_options(options.clone()).await {
                                Ok(new_stream) => {
                                    change_stream = new_stream;
                                    info!("Reconnected change stream for entity: {}", entity.name);
                                }
                                Err(reconnect_err) => {
                                    error!("Failed to reconnect change stream for entity {}: {}", entity.name, reconnect_err);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Update active streams count
        {
            let mut state = connection_state.write().await;
            state.active_streams = state.active_streams.saturating_sub(1);
        }

        info!("Change stream monitoring ended for entity: {}", entity.name);
        Ok(())
    }

    /// Process a change stream event
    async fn process_change_event(
        entity: &Entity,
        change_event: ChangeStreamEvent<Document>,
        oplog_sender: &mpsc::Sender<Oplog>,
    ) -> Result<()> {
        let operation = match change_event.operation_type {
            OperationType::Insert | OperationType::Update | OperationType::Replace => {
                Operation::Upsert
            }
            OperationType::Delete => Operation::Delete,
            _ => {
                debug!(
                    "Ignoring change event type: {:?}",
                    change_event.operation_type
                );
                return Ok(());
            }
        };

        // Extract document ID
        let doc_id = match &change_event.document_key {
            Some(key) => {
                if let Some(Bson::ObjectId(id)) = key.get("_id") {
                    id.to_hex()
                } else if let Some(id_bson) = key.get("_id") {
                    format!("{id_bson}")
                } else {
                    warn!("Change event missing _id in document_key");
                    return Ok(());
                }
            }
            None => {
                warn!("Change event missing document_key");
                return Ok(());
            }
        };

        // Extract document data
        let data = match operation {
            Operation::Upsert => {
                // For upserts, use full_document if available
                if let Some(doc) = &change_event.full_document {
                    serde_json::to_value(doc)?
                } else {
                    serde_json::Value::Null
                }
            }
            Operation::Delete => {
                // For deletes, we only need the document ID
                serde_json::json!({"_id": &doc_id})
            }
        };

        // Create oplog entry
        let oplog = Oplog {
            id: 0, // Will be set by database
            operation,
            doc_id: doc_id.clone(),
            entity: entity.name.clone(),
            data,
            created_at: chrono::Utc::now(),
        };

        // Send to oplog processor
        if let Err(e) = oplog_sender.send(oplog).await {
            error!("Failed to send oplog for entity {}: {}", entity.name, e);
        } else {
            debug!(
                "Processed change event for entity: {} (doc_id: {})",
                entity.name, doc_id
            );
        }

        Ok(())
    }

    /// Scan collection for initial data load
    pub async fn scan_collection(
        &self,
        entity: &Entity,
        oplog_sender: mpsc::Sender<Oplog>,
        batch_size: usize,
    ) -> Result<u64> {
        info!("Starting collection scan for entity: {}", entity.name);

        let collection = self
            .get_collection(entity)
            .ok_or_else(|| anyhow::anyhow!("MongoDB not connected"))?;

        let mut cursor = collection.find(doc! {}).await?;
        let mut processed_count = 0u64;
        let mut batch = Vec::new();

        while cursor.advance().await? {
            let document = cursor.current();

            // Extract document ID
            let doc_id = if let Ok(Some(id_ref)) = document.get("_id") {
                if let Ok(Bson::ObjectId(id)) = id_ref.try_into() {
                    id.to_hex()
                } else {
                    format!("{id_ref:?}")
                }
            } else {
                format!("unknown_{processed_count}")
            };

            // Convert document to JSON
            let data = serde_json::to_value(document)?;

            // Create oplog entry for initial scan
            let oplog = Oplog {
                id: 0, // Will be set by database
                operation: Operation::Upsert,
                doc_id,
                entity: entity.name.clone(),
                data,
                created_at: chrono::Utc::now(),
            };

            batch.push(oplog);
            processed_count += 1;

            // Send batch when it reaches the batch size
            if batch.len() >= batch_size {
                for oplog in batch.drain(..) {
                    if let Err(e) = oplog_sender.send(oplog).await {
                        error!(
                            "Failed to send scan oplog for entity {}: {}",
                            entity.name, e
                        );
                        return Err(e.into());
                    }
                }

                debug!(
                    "Processed {} documents for entity: {}",
                    processed_count, entity.name
                );
            }
        }

        // Send remaining documents in the batch
        for oplog in batch {
            if let Err(e) = oplog_sender.send(oplog).await {
                error!(
                    "Failed to send final scan oplog for entity {}: {}",
                    entity.name, e
                );
                return Err(e.into());
            }
        }

        info!(
            "Completed collection scan for entity: {} ({} documents)",
            entity.name, processed_count
        );
        Ok(processed_count)
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        let state = self.connection_state.read().await;
        state.is_connected
    }

    /// Get connection statistics
    pub async fn get_connection_stats(&self) -> ConnectionStats {
        let state = self.connection_state.read().await;
        ConnectionStats {
            is_connected: state.is_connected,
            connection_failures: state.connection_failures,
            database_name: self.database_name.clone(),
            active_streams: state.active_streams,
        }
    }

    /// Handle connection failure
    pub async fn handle_connection_failure(&self) {
        let mut state = self.connection_state.write().await;
        state.is_connected = false;
        state.connection_failures += 1;

        warn!(
            "MongoDB connection failure (count: {})",
            state.connection_failures
        );
    }

    /// Health check
    pub async fn health_check(&self) -> Result<()> {
        if let Some(client) = &self.client {
            client
                .database(&self.database_name)
                .run_command(doc! {"ping": 1})
                .await?;

            debug!("MongoDB health check passed");
            Ok(())
        } else {
            Err(anyhow::anyhow!("MongoDB client not connected"))
        }
    }
}

impl ChangeStreamHandle {
    /// Stop the change stream
    pub async fn stop(self) -> Result<()> {
        info!("Stopping change stream for entity: {}", self.entity_name);
        self.stop_sender.send(()).await?;
        Ok(())
    }

    /// Get entity name
    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }
}

/// Connection statistics for monitoring
#[derive(Debug, serde::Serialize)]
pub struct ConnectionStats {
    pub is_connected: bool,
    pub connection_failures: u64,
    pub database_name: String,
    pub active_streams: u64,
}
