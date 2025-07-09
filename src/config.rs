use std::{collections::HashMap, time::Duration};

use anyhow::{Context, bail};
use config::{Config, Environment, File};
use serde::{Deserialize, Serialize, de};

fn deserialize_secs<'de, D>(s: D) -> Result<Duration, D::Error>
where
    D: de::Deserializer<'de>,
{
    let duration_secs: f64 = de::Deserialize::deserialize(s)?;
    Ok(Duration::from_secs_f64(duration_secs))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    #[serde(deserialize_with = "deserialize_secs")]
    pub shutdown_timeout: Duration,
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
    #[serde(deserialize_with = "deserialize_secs")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub base_dir: String,
    #[serde(deserialize_with = "deserialize_secs")]
    pub replication_interval: Duration,
    #[serde(deserialize_with = "deserialize_secs")]
    pub entity_refresh_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    #[serde(deserialize_with = "deserialize_secs")]
    pub check_interval: Duration,
    pub min_lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Configs {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub database: DatabaseConfig,
    pub sources: HashMap<String, SourceConfig>,
    pub cache: CacheConfig,
    pub snapshot: SnapshotConfig,
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
