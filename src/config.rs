use std::collections::HashMap;

use anyhow::{Context, bail};
use config::{Config, Environment, File};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub uri: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub connection_timeout: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub base_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub database: DatabaseConfig,
    pub sources: HashMap<String, SourceConfig>,
    pub cache: CacheConfig,
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        let config = Config::builder()
            .add_source(File::with_name("config").required(false))
            .add_source(Environment::with_prefix("EMCACHE").separator("_"))
            .build()
            .context("Failed to build config")?;

        match config.try_deserialize::<AppConfig>() {
            Ok(config) => Ok(config),
            Err(error) => bail!("Failed to load config: {}", error),
        }
    }
}
