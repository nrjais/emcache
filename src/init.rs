use anyhow::Result;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing_subscriber::EnvFilter;

use crate::api::ApiServer;
use crate::config::AppConfig;
use crate::entity::EntityManager;
use crate::executor::TaskServer;
use crate::mongo::{MongoClient, ResumeTokenManager};
use crate::oplog::{OplogDatabase, OplogManager};
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
        let oplog_db = OplogDatabase::new(postgres_client.clone());

        let (oplog_ack_sender, oplog_ack_receiver) = broadcast::channel(10);

        let (oplog_manager, oplog_sender) = OplogManager::new(postgres_client.clone(), oplog_ack_sender).await?;
        let resume_token_manager = Arc::new(ResumeTokenManager::new(postgres_client.clone(), oplog_ack_receiver));

        let mongo_client = MongoClient::new(
            &conf,
            &postgres_client,
            oplog_sender,
            entity_manager.clone(),
            resume_token_manager.clone(),
        )
        .await?;

        let meta = metadata_sqlite(&conf).await?;
        let metadata_db = MetadataDb::new(meta.clone());

        entity_manager.init().await?;
        metadata_db.init().await?;

        let replicator = Replicator::new(&conf, postgres_client.clone(), entity_manager.clone(), metadata_db);

        let task_server = TaskServer::new();

        register_tasks(
            oplog_manager,
            mongo_client,
            replicator,
            &entity_manager,
            &task_server,
            resume_token_manager,
        )
        .await?;

        let api_server = ApiServer::new(conf.clone(), entity_manager, oplog_db);

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
    entity_manager: &Arc<EntityManager>,
    task_server: &TaskServer,
    resume_token_manager: Arc<ResumeTokenManager>,
) -> Result<(), anyhow::Error> {
    task_server.register(mongo_client).await?;
    task_server.register(oplog_manager).await?;
    task_server.register(replicator).await?;
    task_server.register(Arc::clone(entity_manager)).await?;
    task_server.register(resume_token_manager).await?;
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
