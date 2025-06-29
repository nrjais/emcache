use anyhow::Result;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod api;
mod config;
pub mod entity;
mod job_server;
pub mod mongo;
mod oplog;
pub mod replicator;
pub mod storage;
mod types;
mod utils;

use crate::api::ApiServer;
use crate::config::AppConfig;
use crate::entity::EntityManager;
use crate::oplog::OplogManager;
use crate::storage::PostgresClient;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load().expect("Failed to load configuration");
    setup_logging(&config.logging.level);
    info!("Starting EMCache server");

    info!("Configuration loaded successfully: {:#?}", config);

    info!("Initializing database connection...");
    let postgres_client = PostgresClient::new(config.clone()).await?;
    info!("Database connection established");

    info!("Initializing entity manager...");
    let entity_manager = Arc::new(EntityManager::new(postgres_client.clone()));

    info!("Initializing oplog manager...");
    let oplog_manager =
        Arc::new(OplogManager::new(config.clone(), postgres_client, Arc::clone(&entity_manager)).await?);

    info!("Initializing API server...");
    let api_server = ApiServer::new(config, entity_manager, oplog_manager);

    info!("Starting API server with graceful shutdown handling...");


    let shutdown_signal = shutdown_signal();
    tokio::select! {
        result = api_server.start() => {
            if let Err(e) = result {
                error!("API server error: {}", e);
                return Err(e);
            }
        }
        _ = shutdown_signal => {
            info!("Graceful shutdown completed");
        }
    }

    Ok(())
}

fn setup_logging(level: &str) {
    let env_filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
