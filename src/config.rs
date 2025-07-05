use anyhow::Result;
use config::{Config, ConfigError, Environment, File};
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
    pub entity: String,
    pub collection: String,
    pub database: String,
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
            },
            database: DatabaseConfig {
                postgres: PostgresConfig {
                    uri: "postgresql://postgres:password@postgres.emcache.orb.local:5432/emcache".to_string(),
                    max_connections: 20,
                    min_connections: 5,
                    connection_timeout: 1000,
                },
            },
            sources: SourcesConfig {
                main: SourceConfig {
                    uri: "mongodb://mongo.emcache.orb.local:27017/test?directConnection=true".to_string(),
                    entity: "test_entity".to_string(),
                    collection: "test_collection".to_string(),
                    database: "test_database".to_string(),
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
}
