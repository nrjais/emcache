use anyhow::Result;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing_subscriber::EnvFilter;

use crate::api::ApiServer;
use crate::config::Configs;
use crate::entity::EntityManager;
use crate::executor::TaskServer;
use crate::mongo::{MongoClient, ResumeTokenManager};
use crate::oplog::{OplogCleanupTask, OplogDatabase, OplogManager};
use crate::replicator::Replicator;
use crate::replicator::metadata::MetadataDb;
use crate::replicator::sqlite::SqliteManager;
use crate::snapshot::SnapshotManager;
use crate::storage::PostgresClient;

pub struct Systems {
    pub api_server: ApiServer,
    pub task_server: TaskServer,
}

impl Systems {
    pub async fn init(conf: &Configs) -> Result<Self> {
        init_base_dir(&conf.cache.base_dir)?;

        let postgres_client = PostgresClient::new(conf.clone()).await?;

        let entity_manager = Arc::new(EntityManager::new(
            postgres_client.clone(),
            conf.cache.entity_refresh_interval,
        ));
        let oplog_db = OplogDatabase::new(postgres_client.clone());

        let (oplog_ack_sender, oplog_ack_receiver) = broadcast::channel(10);

        let (oplog_manager, oplog_sender) = OplogManager::new(oplog_db.clone(), oplog_ack_sender).await?;
        let resume_token_manager = Arc::new(ResumeTokenManager::new(postgres_client.clone(), oplog_ack_receiver));
        let sqlite_manager = Arc::new(SqliteManager::new(&conf.cache.base_dir));

        let mongo_client =
            MongoClient::new(conf, oplog_sender, entity_manager.clone(), resume_token_manager.clone()).await?;

        let metadata_db = MetadataDb::new(&conf.cache.base_dir)?;

        entity_manager.init().await?;

        let replicator = Replicator::new(
            entity_manager.clone(),
            metadata_db,
            oplog_db.clone(),
            sqlite_manager.clone(),
            conf.cache.replication_interval,
        );

        let snapshot_manager = Arc::new(SnapshotManager::new(
            conf,
            entity_manager.clone(),
            sqlite_manager.clone(),
        ));

        snapshot_manager.init().await?;

        let oplog_cleanup_task = OplogCleanupTask::new(
            postgres_client.clone(),
            entity_manager.clone(),
            conf.oplog.cleanup_interval,
            conf.oplog.retention_days,
        );

        let task_server = TaskServer::new();

        register_tasks(
            oplog_manager,
            mongo_client,
            replicator,
            &entity_manager,
            &task_server,
            resume_token_manager,
            &snapshot_manager,
            oplog_cleanup_task,
        )
        .await?;

        let api_server = ApiServer::new(conf.clone(), entity_manager, oplog_db, snapshot_manager);

        Ok(Systems {
            api_server,
            task_server,
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn register_tasks(
    oplog_manager: OplogManager,
    mongo_client: MongoClient,
    replicator: Replicator,
    entity_manager: &Arc<EntityManager>,
    task_server: &TaskServer,
    resume_token_manager: Arc<ResumeTokenManager>,
    snapshot_manager: &Arc<SnapshotManager>,
    oplog_cleanup_task: OplogCleanupTask,
) -> Result<(), anyhow::Error> {
    task_server.register(mongo_client).await?;
    task_server.register(oplog_manager).await?;
    task_server.register(replicator).await?;
    task_server.register(Arc::clone(entity_manager)).await?;
    task_server.register(resume_token_manager).await?;
    task_server.register(Arc::clone(snapshot_manager)).await?;
    task_server.register(oplog_cleanup_task).await?;
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
