use anyhow::Result;
use tracing::info;
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

use crate::config::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load().expect("Failed to load configuration");
    setup_logging(&config.logging.level);
    info!("Starting EMCache server");

    info!("Configuration loaded successfully: {:#?}", config);

    Ok(())
}

fn setup_logging(level: &str) {
    let env_filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().pretty().with_env_filter(env_filter).init();
}
