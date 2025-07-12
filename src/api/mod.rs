pub mod entities;
pub mod health;
pub mod oplogs;
mod snapshot;
mod with_rejection;

use anyhow::Result;
use axum::{Router, routing::get};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::compression::CompressionLayer;
use tracing::info;

use crate::config::Configs;
use crate::entity::EntityManager;
use crate::oplog::OplogDatabase;
use crate::snapshot::SnapshotManager;

pub struct ApiServer {
    config: Configs,
    entity_manager: Arc<EntityManager>,
    oplog_db: OplogDatabase,
    snapshot_manager: Arc<SnapshotManager>,
}

impl ApiServer {
    pub fn new(
        config: Configs,
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
            config: self.config.clone(),
        };

        let app = Router::new()
            .route("/health", get(health::health_check))
            .merge(entities::router())
            .merge(oplogs::router())
            .merge(snapshot::router())
            .layer(CompressionLayer::new().gzip(true).zstd(true))
            .with_state(state);

        let app = Router::new().nest("/api", app);

        let bind_addr = format!("{}:{}", self.config.server.host, self.config.server.port);
        info!("Starting API server on {}", bind_addr);

        let listener = TcpListener::bind(&bind_addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub entity_manager: Arc<EntityManager>,
    pub oplog_db: OplogDatabase,
    pub snapshot_manager: Arc<SnapshotManager>,
    pub config: Configs,
}
