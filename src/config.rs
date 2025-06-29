use anyhow::Result;
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesConfig {
    pub main: SourceConfig,
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
    pub sources: SourcesConfig,
    pub cache: CacheConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "pretty".to_string(),
            },
            database: DatabaseConfig {
                postgres: PostgresConfig {
                    uri: "postgresql://emcachers:password@localhost:5432/emcachers".to_string(),
                    max_connections: 20,
                    min_connections: 5,
                    connection_timeout: 1000,
                },
            },
            sources: SourcesConfig {
                main: SourceConfig {
                    uri: "mongodb://localhost:27017/test".to_string(),
                },
            },
            cache: CacheConfig {
                base_dir: "caches".to_string(),
            },
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name("config").required(false))
            .add_source(Environment::with_prefix("EMCACHE").separator("_"))
            .build()?;

        match config.try_deserialize::<AppConfig>() {
            Ok(config) => Ok(config),
            Err(_) => Ok(AppConfig::default()),
        }
    }

    pub fn connection_timeout_duration(&self) -> Duration {
        Duration::from_millis(self.database.postgres.connection_timeout)
    }

    pub fn mongodb_uri(&self) -> &str {
        &self.sources.main.uri
    }

    pub fn postgres_uri(&self) -> &str {
        &self.database.postgres.uri
    }
}
