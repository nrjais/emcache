use anyhow::Result;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use crate::api::ApiServer;
use crate::config::AppConfig;
use crate::entity::EntityManager;
use crate::executor::TaskServer;
use crate::mongo::MongoClient;
use crate::oplog::OplogManager;
use crate::replicator::Replicator;
use crate::replicator::metadata::MetadataDb;
use crate::storage::{PostgresClient, metadata_sqlite};

pub struct Systems {
    pub api_server: ApiServer,
    pub task_server: TaskServer,
}

impl Systems {
    pub async fn init(conf: AppConfig) -> Result<Self> {
        init_base_dir(&conf.cache.base_dir)?;

        let postgres_client = PostgresClient::new(conf.clone()).await?;

        let entity_manager = Arc::new(EntityManager::new(postgres_client.clone()));

        let (oplog_manager, oplog_sender) = OplogManager::new(postgres_client.clone()).await?;
        let mongo_client = MongoClient::new(&conf, &postgres_client, oplog_sender).await?;

        let meta = metadata_sqlite(&conf).await?;
        let metadata_db = MetadataDb::new(meta.clone());
        metadata_db.init().await?;

        let replicator = Replicator::new(&conf, postgres_client.clone(), entity_manager.clone(), metadata_db);

        let task_server = TaskServer::new();

        register_tasks(oplog_manager, mongo_client, replicator, &task_server).await?;

        let api_server = ApiServer::new(conf.clone(), Arc::clone(&entity_manager));

        Ok(Systems {
            api_server,
            task_server,
        })
    }
}

async fn register_tasks(
    oplog_manager: OplogManager,
    mongo_client: MongoClient,
    replicator: Replicator,
    task_server: &TaskServer,
) -> Result<(), anyhow::Error> {
    task_server.register(mongo_client).await?;
    task_server.register(oplog_manager).await?;
    task_server.register(replicator).await?;
    Ok(())
}

pub fn setup_logging(level: &str) {
    let env_filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}

fn init_base_dir(base_dir: &str) -> Result<()> {
    if !Path::new(base_dir).exists() {
        std::fs::create_dir_all(base_dir)?;
    }

    if !Path::new(base_dir).is_dir() {
        return Err(anyhow::anyhow!("Base directory is not a directory"));
    }

    // Check if the base directory is readable and writable
    let _ = fs::read_dir(base_dir)?;

    // Check if the base directory is empty
    if fs::read_dir(base_dir)?.next().is_none() {
        std::fs::write(Path::new(base_dir).join("test.txt"), "")?;
        std::fs::remove_file(Path::new(base_dir).join("test.txt"))?;
    }

    Ok(())
}
