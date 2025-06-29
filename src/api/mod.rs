pub mod entities;
pub mod health;
pub mod oplogs;

// Re-exports not needed since modules are used directly in routes

use anyhow::Result;
use axum::{Router, routing::get};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::AppConfig;
use crate::entity::EntityManager;
use crate::oplog::OplogManager;

/// API server for EMCachers management
pub struct ApiServer {
    config: AppConfig,
    entity_manager: Arc<EntityManager>,
    oplog_manager: Arc<OplogManager>,
}

impl ApiServer {
    pub fn new(config: AppConfig, entity_manager: Arc<EntityManager>, oplog_manager: Arc<OplogManager>) -> Self {
        Self {
            config,
            entity_manager,
            oplog_manager,
        }
    }

    pub async fn start(&self) -> Result<()> {
        let state = AppState {
            entity_manager: Arc::clone(&self.entity_manager),
            oplog_manager: Arc::clone(&self.oplog_manager),
        };

        let app = Router::new()
            .route("/health", get(health::health_check))
            .route("/health/detailed", get(health::detailed_health_check))
            .merge(entities::router())
            .merge(oplogs::router())
            .with_state(state);

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
    pub oplog_manager: Arc<OplogManager>,
}
