use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info, warn};

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

use crate::config::AppConfig;
use crate::job_server::JobServer;

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

    // Initialize job server and register jobs
    let job_server = Arc::new(JobServer::new());

    // Start API server in background
    info!("API server started");

    // Set up graceful shutdown
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token_clone = shutdown_token.clone();

    // Handle shutdown signals
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
        info!("Shutdown signal received");
        shutdown_token_clone.cancel();
    });

    info!("EMCachers server fully initialized and running");

    // Wait for shutdown signal
    shutdown_token.cancelled().await;
    info!("Initiating graceful shutdown");

    // Stop all components
    if let Err(e) = job_server.shutdown().await {
        error!("Error shutting down job server: {}", e);
    }

    info!("EMCachers server shutdown complete");
    Ok(())
}
