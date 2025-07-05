use std::time::Duration;

use anyhow::Result;
use tokio::signal;
use tracing::{error, info};

mod api;
mod config;
pub mod entity;
mod executor;
mod init;
pub mod mongo;
mod oplog;
pub mod replicator;
pub mod storage;
mod types;

use crate::{config::AppConfig, init::Systems};

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load().expect("Failed to load configuration");
    init::setup_logging(&config.logging.level);
    info!("Configuration loaded successfully: {:#?}", config);

    let systems = Systems::init(config).await?;
    info!("Starting EMCache server");

    let shutdown_signal = shutdown_signal();
    tokio::select! {
        result = systems.api_server.start() => {
            if let Err(e) = result {
                error!("API server error: {}", e);
                return Err(e);
            }
        }
        _ = shutdown_signal => {
            info!("Shutting down task server");
            tokio::select! {
                _ = systems.task_server.shutdown() => {
                    info!("Task server shutdown completed");
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    info!("Shutting down task server timed out, force killing");
                }
            }
        }
    }

    Ok(())
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
