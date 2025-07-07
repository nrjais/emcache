pub mod entities;
pub mod health;
pub mod oplogs;
mod snapshot;

// Re-exports not needed since modules are used directly in routes

use anyhow::Result;
use axum::{Router, routing::get};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::AppConfig;
use crate::entity::EntityManager;
use crate::oplog::OplogDatabase;
use crate::snapshot::SnapshotManager;

/// API server for EMCachers management
pub struct ApiServer {
    config: AppConfig,
    entity_manager: Arc<EntityManager>,
    oplog_db: OplogDatabase,
    snapshot_manager: Arc<SnapshotManager>,
}

impl ApiServer {
    pub fn new(
        config: AppConfig,
        entity_manager: Arc<EntityManager>,
        oplog_db: OplogDatabase,
        snapshot_manager: Arc<SnapshotManager>,
    ) -> Self {
        Self {
            config,
            entity_manager,
            oplog_db,
            snapshot_manager,
        }
    }

    pub async fn start(&self) -> Result<()> {
        let state = AppState {
            entity_manager: Arc::clone(&self.entity_manager),
            oplog_db: self.oplog_db.clone(),
            snapshot_manager: Arc::clone(&self.snapshot_manager),
        };

        let app = Router::new()
            .route("/health", get(health::health_check))
            .route("/health/detailed", get(health::detailed_health_check))
            .merge(entities::router())
            .merge(oplogs::router())
            .merge(snapshot::router())
            .with_state(state);

        let app = Router::new().nest("/api", app);

        let bind_addr = format!("{}:{}", self.config.server.host, self.config.server.port);
        info!("Starting API server on {}", bind_addr);

        let listener = TcpListener::bind(&bind_addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
}

/// Application state for handlers
#[derive(Clone)]
pub struct AppState {
    pub entity_manager: Arc<EntityManager>,
    pub oplog_db: OplogDatabase,
    pub snapshot_manager: Arc<SnapshotManager>,
}
