pub mod entities;
pub mod health;
pub mod oplogs;
pub mod replication;
pub mod system;

// Re-exports not needed since modules are used directly in routes

use anyhow::Result;
use axum::{routing::get, Router};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use crate::cache_replicator::CacheReplicator;
use crate::config::AppConfig;
use crate::database::DatabaseManager;
use crate::entity_manager::EntityManager;
use crate::oplog::OplogManager;

/// API server for EMCachers management
pub struct ApiServer {
    config: AppConfig,
    db_manager: Arc<DatabaseManager>,
    entity_manager: Arc<EntityManager>,
    oplog_manager: Arc<OplogManager>,
    cache_replicator: Arc<CacheReplicator>,
}

impl ApiServer {
    /// Create a new API server
    pub fn new(
        config: AppConfig,
        db_manager: Arc<DatabaseManager>,
        entity_manager: Arc<EntityManager>,
        oplog_manager: Arc<OplogManager>,
        cache_replicator: Arc<CacheReplicator>,
    ) -> Self {
        Self {
            config,
            db_manager,
            entity_manager,
            oplog_manager,
            cache_replicator,
        }
    }

    /// Start the API server
    pub async fn start(&self) -> Result<()> {
        let state = AppState {
            db_manager: Arc::clone(&self.db_manager),
            entity_manager: Arc::clone(&self.entity_manager),
            oplog_manager: Arc::clone(&self.oplog_manager),
            cache_replicator: Arc::clone(&self.cache_replicator),
        };

        let app = Router::new()
            // Health check endpoints
            .route("/health", get(health::health_check))
            .route("/health/detailed", get(health::detailed_health_check))
            // Entity management endpoints
            .merge(entities::router())
            // Oplog endpoints
            .merge(oplogs::router())
            // Cache replication endpoints
            .merge(replication::router())
            // System endpoints
            .merge(system::router())
            .with_state(state);

        let bind_addr = format!("{}:{}", self.config.api.host, self.config.api.port);
        info!("Starting API server on {}", bind_addr);

        let listener = TcpListener::bind(&bind_addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
}

/// Application state for handlers
#[derive(Clone)]
pub struct AppState {
    pub db_manager: Arc<DatabaseManager>,
    pub entity_manager: Arc<EntityManager>,
    pub oplog_manager: Arc<OplogManager>,
    pub cache_replicator: Arc<CacheReplicator>,
}
