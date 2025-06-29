use anyhow::Result;
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub mongodb_url: String,
    pub postgres_url: String,
    pub sqlite_base_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    pub entity_refresh_interval: u64,
    pub oplog_scan_interval: u64,
    pub cache_replication_interval: u64,
    pub connection_cleanup_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub default_batch_size: usize,
    pub mongo_batch_size: usize,
    pub postgres_batch_size: usize,
    pub oplog_batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolConfig {
    pub postgres_max_connections: u32,
    pub postgres_min_connections: u32,
    pub sqlite_max_connections: u32,
    pub connection_timeout: u64,
    pub idle_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_backoff: u64,
    pub max_backoff: u64,
    pub backoff_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub host: String,
    pub port: u16,
    pub request_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub jobs: JobConfig,
    pub batch: BatchConfig,
    pub connection_pool: ConnectionPoolConfig,
    pub retry: RetryConfig,
    pub api: ApiConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig {
                mongodb_url: "mongodb://localhost:27017/emcachers".to_string(),
                postgres_url: "postgresql://postgres:password@postgres.emcache.orb.local:5432/emcache?sslmode=disable"
                    .to_string(),
                sqlite_base_path: "./dbs".to_string(),
            },
            jobs: JobConfig {
                entity_refresh_interval: 300,     // 5 minutes
                oplog_scan_interval: 30,          // 30 seconds
                cache_replication_interval: 10,   // 10 seconds
                connection_cleanup_interval: 600, // 10 minutes
            },
            batch: BatchConfig {
                default_batch_size: 1000,
                mongo_batch_size: 1000,
                postgres_batch_size: 1000,
                oplog_batch_size: 1000,
            },
            connection_pool: ConnectionPoolConfig {
                postgres_max_connections: 10,
                postgres_min_connections: 2,
                sqlite_max_connections: 5,
                connection_timeout: 30,
                idle_timeout: 300,
            },
            retry: RetryConfig {
                max_retries: 5,
                initial_backoff: 1000,
                max_backoff: 60000,
                backoff_multiplier: 2.0,
            },
            api: ApiConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
                request_timeout: 30,
            },
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name("config").required(false))
            .add_source(Environment::with_prefix("EMCACHERS").separator("_"))
            .build()?;

        // Try to deserialize, fall back to defaults if not found
        match config.try_deserialize::<AppConfig>() {
            Ok(config) => Ok(config),
            Err(_) => {
                // If config file doesn't exist or is incomplete, use defaults
                Ok(AppConfig::default())
            }
        }
    }

    pub fn entity_refresh_duration(&self) -> Duration {
        Duration::from_secs(self.jobs.entity_refresh_interval)
    }

    pub fn oplog_scan_duration(&self) -> Duration {
        Duration::from_secs(self.jobs.oplog_scan_interval)
    }

    pub fn cache_replication_duration(&self) -> Duration {
        Duration::from_secs(self.jobs.cache_replication_interval)
    }

    pub fn connection_cleanup_duration(&self) -> Duration {
        Duration::from_secs(self.jobs.connection_cleanup_interval)
    }

    pub fn connection_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.connection_pool.connection_timeout)
    }

    pub fn idle_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.connection_pool.idle_timeout)
    }
}
