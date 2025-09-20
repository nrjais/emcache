use std::{collections::HashMap, time::Duration};

use anyhow::{Context, bail};
use config::{Config, Environment, File};
use serde::{Deserialize, Serialize};
use serde_with::{DurationSecondsWithFrac, serde_as};

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub shutdown_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub uri: String,
    pub max_connections: u32,
    pub min_connections: u32,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub connection_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub postgres: PostgresConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub uri: String,
    pub database: String,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub base_dir: String,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub replication_interval: Duration,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub entity_refresh_interval: Duration,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub check_interval: Duration,
    pub min_lag: u64,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OplogConfig {
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub cleanup_interval: Duration,
    pub retention_days: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Configs {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub database: DatabaseConfig,
    pub sources: HashMap<String, SourceConfig>,
    pub cache: CacheConfig,
    pub snapshot: SnapshotConfig,
    pub oplog: OplogConfig,
}

impl Configs {
    pub fn load() -> anyhow::Result<Self> {
        let config = Config::builder()
            .add_source(File::with_name("config").required(false))
            .add_source(Environment::with_prefix("EMCACHE").separator("_"))
            .build()
            .context("Failed to build config")?;

        match config.try_deserialize::<Configs>() {
            Ok(config) => Ok(config),
            Err(error) => bail!("Failed to load config: {}", error),
        }
    }
}
