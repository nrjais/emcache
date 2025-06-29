use anyhow::Result;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::api::ApiServer;
use crate::config::AppConfig;
use crate::entity::EntityManager;
use crate::oplog::OplogManager;
use crate::storage::PostgresClient;

pub struct Systems {
    pub config: AppConfig,
    pub postgres_client: PostgresClient,
    pub entity_manager: Arc<EntityManager>,
    pub oplog_manager: Arc<OplogManager>,
    pub api_server: ApiServer,
}

impl Systems {
    pub async fn init(conf: AppConfig) -> Result<Self> {
        let postgres_client = PostgresClient::new(conf.clone()).await?;

        let entity_manager = Arc::new(EntityManager::new(postgres_client.clone()));

        let oplog_manager =
            Arc::new(OplogManager::new(conf.clone(), postgres_client.clone(), Arc::clone(&entity_manager)).await?);

        let api_server = ApiServer::new(conf.clone(), Arc::clone(&entity_manager), Arc::clone(&oplog_manager));

        Ok(Systems {
            config: conf,
            postgres_client,
            entity_manager,
            oplog_manager,
            api_server,
        })
    }
}

pub fn setup_logging(level: &str) {
    let env_filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}
