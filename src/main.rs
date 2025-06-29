use anyhow::Result;
use tracing::info;

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

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("Starting EMCachers server");

    // Load configuration
    let config = AppConfig::load().expect("Failed to load configuration");

    info!("Configuration loaded successfully: {:?}", config);

    Ok(())
}
