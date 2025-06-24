use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::{error, info, warn};

mod api;
mod cache_db_manager;
mod cache_replicator;
mod config;
mod database;
mod database_cleanup_job;
mod entity_manager;
mod job_server;
pub mod mongo;
mod mongo_client;
mod oplog;
mod resume_token_manager;
pub mod storage;
mod types;
mod utils;

use crate::cache_db_manager::CacheDbManager;
use crate::cache_replicator::CacheReplicator;
use crate::config::AppConfig;
use crate::database::DatabaseManager;
use crate::database_cleanup_job::DatabaseCleanupJob;
use crate::entity_manager::EntityManager;
use crate::job_server::{JobServer, create_job_config};
use crate::mongo_client::{ChangeStreamHandle, MongoClient};
use crate::resume_token_manager::ResumeTokenManager;
use types::{Entity, Oplog};

/// Change stream manager for handling MongoDB change streams
pub struct ChangeStreamManager {
    mongo_client: MongoClient,
    oplog_sender: mpsc::Sender<Oplog>,
    resume_token_manager: Arc<ResumeTokenManager>,
    active_streams: Arc<RwLock<Vec<ChangeStreamHandle>>>,
}

impl ChangeStreamManager {
    pub fn new(
        mongo_client: MongoClient,
        oplog_sender: mpsc::Sender<Oplog>,
        resume_token_manager: Arc<ResumeTokenManager>,
    ) -> Self {
        Self {
            mongo_client,
            oplog_sender,
            resume_token_manager,
            active_streams: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Start change streams for all entities
    pub async fn start_change_streams(&self, entities: &[Entity]) -> Result<()> {
        info!("Starting change streams for {} entities", entities.len());

        let mut handles = Vec::new();

        for entity in entities {
            match self.start_entity_change_stream(entity).await {
                Ok(handle) => {
                    info!("Started change stream for entity: {}", entity.name);
                    handles.push(handle);
                }
                Err(e) => {
                    error!(
                        "Failed to start change stream for entity {}: {}",
                        entity.name, e
                    );
                    // Continue with other entities
                }
            }
        }

        // Store active stream handles
        {
            let mut active_streams = self.active_streams.write().await;
            active_streams.extend(handles);
        }

        info!(
            "Change stream manager started with {} active streams",
            self.active_streams.read().await.len()
        );
        Ok(())
    }

    /// Start change stream for a single entity
    async fn start_entity_change_stream(&self, entity: &Entity) -> Result<ChangeStreamHandle> {
        // Check if entity needs initial scan
        let resume_token = match self.resume_token_manager.get_token(&entity.name).await {
            Ok(Some(token)) => {
                info!("Using resume token for entity: {}", entity.name);
                Some(token.token_data)
            }
            Ok(None) => {
                info!(
                    "No resume token found for entity: {}, will perform initial scan",
                    entity.name
                );

                // Perform initial scan before starting change stream
                match self.perform_initial_scan(entity).await {
                    Ok(_) => {
                        info!("Initial scan completed for entity: {}", entity.name);
                    }
                    Err(e) => {
                        error!("Initial scan failed for entity {}: {}", entity.name, e);
                        return Err(e);
                    }
                }
                None
            }
            Err(e) => {
                warn!(
                    "Failed to get resume token for entity {}: {}",
                    entity.name, e
                );
                None
            }
        };

        // Start the change stream
        self.mongo_client
            .start_change_stream(entity, self.oplog_sender.clone(), resume_token)
            .await
    }

    /// Perform initial scan for an entity
    async fn perform_initial_scan(&self, entity: &Entity) -> Result<()> {
        info!("Starting initial scan for entity: {}", entity.name);

        let batch_size = 1000; // Use default batch size
        let scanned_count = self
            .mongo_client
            .scan_collection(entity, self.oplog_sender.clone(), batch_size)
            .await?;

        info!(
            "Initial scan completed for entity {}: {} documents",
            entity.name, scanned_count
        );
        Ok(())
    }

    /// Stop all change streams
    pub async fn stop_all_streams(&self) -> Result<()> {
        info!("Stopping all change streams");

        let mut active_streams = self.active_streams.write().await;
        let handles = std::mem::take(&mut *active_streams);

        for handle in handles {
            let entity_name = handle.entity_name().to_string();
            if let Err(e) = handle.stop().await {
                error!(
                    "Failed to stop change stream for entity {}: {}",
                    entity_name, e
                );
            }
        }

        info!("All change streams stopped");
        Ok(())
    }

    /// Get active stream count
    pub async fn active_stream_count(&self) -> usize {
        self.active_streams.read().await.len()
    }

    /// Start change stream for a new entity (used when entities are added dynamically)
    pub async fn start_stream_for_new_entity(&self, entity: &Entity) -> Result<()> {
        info!("Starting change stream for new entity: {}", entity.name);

        match self.start_entity_change_stream(entity).await {
            Ok(handle) => {
                info!(
                    "Successfully started change stream for new entity: {}",
                    entity.name
                );

                // Add to active streams
                let mut active_streams = self.active_streams.write().await;
                active_streams.push(handle);

                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to start change stream for new entity {}: {}",
                    entity.name, e
                );
                Err(e)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("Starting EMCachers server");

    // Load configuration
    let config = AppConfig::load().unwrap_or_else(|_| {
        warn!("Failed to load configuration, using default");
        AppConfig::default()
    });

    info!("Configuration loaded successfully");

    // Initialize database manager (migrations run automatically)
    let db_manager = Arc::new(DatabaseManager::new(config.clone()).await?);
    info!("Database manager initialized with migrations completed");

    // Initialize core components
    let entity_manager = Arc::new(EntityManager::new(Arc::clone(&db_manager)));
    let cache_db_manager = Arc::new(CacheDbManager::new());
    let resume_token_manager = Arc::new(ResumeTokenManager::new(Arc::clone(&db_manager)));

    // Initialize MongoDB client
    let mut mongo_client = MongoClient::new(&config).await?;
    mongo_client.connect(&config.database.mongodb_url).await?;
    info!("MongoDB client connected");

    // Set up oplog processing channels
    let (oplog_sender, oplog_receiver) = mpsc::channel::<Oplog>(1000);
    let (ack_sender, mut ack_receiver) = mpsc::channel::<i64>(100);

    // Initialize cache replicator
    let cache_replicator = Arc::new(
        CacheReplicator::new(
            config.clone(),
            Arc::clone(&db_manager),
            Arc::clone(&entity_manager),
            Arc::clone(&cache_db_manager),
        )
        .await?,
    );

    // Start background cache replication
    cache_replicator.start_background_replication().await?;
    info!("Background cache replication started");

    // Initialize change stream manager
    let change_stream_manager = Arc::new(ChangeStreamManager::new(
        mongo_client,
        oplog_sender.clone(),
        Arc::clone(&resume_token_manager),
    ));

    // Initialize job server and register jobs
    let job_server = Arc::new(JobServer::new());

    // Register entity refresh job
    job_server
        .register_job(
            EntityManager::new(Arc::clone(&db_manager)),
            create_job_config(
                "entity_refresh",
                config.entity_refresh_duration(),
                3,
                std::time::Duration::from_secs(5),
            ),
        )
        .await?;
    info!("Entity refresh job registered");

    // Register cache replication job
    job_server
        .register_job(
            CacheReplicator::new(
                config.clone(),
                Arc::clone(&db_manager),
                Arc::clone(&entity_manager),
                Arc::clone(&cache_db_manager),
            )
            .await?,
            create_job_config(
                "cache_replication",
                config.cache_replication_duration(),
                5,
                std::time::Duration::from_secs(2),
            ),
        )
        .await?;
    info!("Cache replication job registered");

    // Register database cleanup job
    let cleanup_job =
        DatabaseCleanupJob::new(Arc::clone(&cache_db_manager), Arc::clone(&entity_manager));
    job_server
        .register_job(
            cleanup_job,
            create_job_config(
                "database_cleanup",
                config.connection_cleanup_duration(),
                3,
                std::time::Duration::from_secs(10),
            ),
        )
        .await?;
    info!("Database cleanup job registered");

    // Load entities and start change streams
    let entities = entity_manager.get_all_entities().await?;
    info!("Loaded {} entities from database", entities.len());

    if !entities.is_empty() {
        change_stream_manager
            .start_change_streams(&entities)
            .await?;
        info!("Change streams started for all entities");
    } else {
        info!("No entities found, change streams will be started when entities are added");
    }

    // Start API server in background
    info!("API server started");

    // Set up graceful shutdown
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token_clone = shutdown_token.clone();

    // Handle shutdown signals
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl+c");
        info!("Shutdown signal received");
        shutdown_token_clone.cancel();
    });

    // Start acknowledgment processing
    tokio::spawn(async move {
        while let Some(ack_count) = ack_receiver.recv().await {
            // In a real implementation, you might want to update resume tokens here
            // or perform other acknowledgment-related tasks
            tracing::debug!("Received acknowledgment for {} oplogs", ack_count);

            // TODO: Implement proper resume token acknowledgment
            // This could involve:
            // 1. Tracking which oplogs were acknowledged
            // 2. Updating resume tokens periodically
            // 3. Notifying MongoDB change streams of successful processing
        }
    });

    info!("EMCachers server fully initialized and running");

    // Wait for shutdown signal
    shutdown_token.cancelled().await;
    info!("Initiating graceful shutdown");

    // Stop all components
    if let Err(e) = change_stream_manager.stop_all_streams().await {
        error!("Error stopping change streams: {}", e);
    }

    if let Err(e) = job_server.shutdown().await {
        error!("Error shutting down job server: {}", e);
    }

    if let Err(e) = cache_db_manager.shutdown().await {
        error!("Error shutting down cache DB manager: {}", e);
    }

    // System monitor doesn't need explicit shutdown

    info!("EMCachers server shutdown complete");
    Ok(())
}
