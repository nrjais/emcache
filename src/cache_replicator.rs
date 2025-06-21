// Cache replicator module

use anyhow::Result;
use serde_json::Value as JsonValue;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::cache_db_manager::CacheDbManager;
use crate::config::AppConfig;
use crate::database::DatabaseManager;
use crate::entity_manager::EntityManager;
use crate::job_server::Job;
use crate::types::{Entity, EntityStatus, Operation};
use crate::utils::RetryExecutor;

/// Cache replicator for applying oplogs to SQLite cache databases
pub struct CacheReplicator {
    name: String,
    config: AppConfig,
    db_manager: Arc<DatabaseManager>,
    entity_manager: Arc<EntityManager>,
    cache_db_manager: Arc<CacheDbManager>,
    replication_state: Arc<RwLock<ReplicationState>>,
    retry_executor: RetryExecutor,
}

/// Replication state tracking
#[derive(Debug)]
struct ReplicationState {
    last_processed_id: i64,
    entity_offsets: HashMap<String, i64>,
    total_processed: u64,
    failed_operations: u64,
    last_processed_at: Option<Instant>,
}

impl CacheReplicator {
    /// Create a new Cache Replicator
    pub async fn new(
        config: AppConfig,
        db_manager: Arc<DatabaseManager>,
        entity_manager: Arc<EntityManager>,
        cache_db_manager: Arc<CacheDbManager>,
    ) -> Result<Self> {
        info!("Initializing Cache Replicator");

        // Load replication state from metadata
        let replication_state = Self::load_replication_state(&db_manager).await?;

        Ok(Self {
            name: "cache_replicator".to_string(),
            config,
            db_manager,
            entity_manager,
            cache_db_manager,
            replication_state: Arc::new(RwLock::new(replication_state)),
            retry_executor: RetryExecutor::new(),
        })
    }

    /// Start replication process
    pub async fn start_replication(&mut self) -> Result<()> {
        info!("Starting cache replication");

        let db_manager = Arc::clone(&self.db_manager);
        let entity_manager = Arc::clone(&self.entity_manager);
        let cache_db_manager = Arc::clone(&self.cache_db_manager);
        let replication_state = Arc::clone(&self.replication_state);
        let retry_executor = self.retry_executor.clone();
        let batch_size = self.config.batch.postgres_batch_size;

        // Spawn background replication task
        tokio::spawn(async move {
            Self::replication_loop(
                db_manager,
                entity_manager,
                cache_db_manager,
                replication_state,
                retry_executor,
                batch_size,
            )
            .await
        });

        info!("Cache replication started");
        Ok(())
    }

    /// Start replication process in background
    pub async fn start_background_replication(&self) -> Result<()> {
        info!("Starting background cache replication");

        let db_manager = Arc::clone(&self.db_manager);
        let entity_manager = Arc::clone(&self.entity_manager);
        let cache_db_manager = Arc::clone(&self.cache_db_manager);
        let replication_state = Arc::clone(&self.replication_state);
        let retry_executor = self.retry_executor.clone();
        let batch_size = self.config.batch.postgres_batch_size;

        // Spawn background replication task
        tokio::spawn(async move {
            Self::replication_loop(
                db_manager,
                entity_manager,
                cache_db_manager,
                replication_state,
                retry_executor,
                batch_size,
            )
            .await
        });

        info!("Background cache replication started");
        Ok(())
    }

    /// Main replication loop
    async fn replication_loop(
        db_manager: Arc<DatabaseManager>,
        entity_manager: Arc<EntityManager>,
        cache_db_manager: Arc<CacheDbManager>,
        replication_state: Arc<RwLock<ReplicationState>>,
        retry_executor: RetryExecutor,
        batch_size: usize,
    ) {
        info!("Starting cache replication loop");

        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;

            // Get current replication state
            let last_processed_id = {
                let state = replication_state.read().await;
                state.last_processed_id
            };

            // Process oplogs with retry logic
            let result = retry_executor
                .execute(|| {
                    Self::process_oplog_batch(
                        Arc::clone(&db_manager),
                        Arc::clone(&entity_manager),
                        Arc::clone(&cache_db_manager),
                        Arc::clone(&replication_state),
                        last_processed_id,
                        batch_size,
                    )
                })
                .await;

            match result {
                Ok(processed_count) => {
                    if processed_count == 0 {
                        // No new oplogs, sleep longer
                        sleep(Duration::from_secs(10)).await;
                    } else {
                        debug!("Processed {} oplogs", processed_count);
                    }
                }
                Err(e) => {
                    error!("Failed to process oplog batch: {}", e);

                    // Update failed operations count
                    {
                        let mut state = replication_state.write().await;
                        state.failed_operations += 1;
                    }

                    sleep(Duration::from_secs(30)).await;
                }
            }
        }
    }

    /// Process a batch of oplogs
    async fn process_oplog_batch(
        db_manager: Arc<DatabaseManager>,
        entity_manager: Arc<EntityManager>,
        cache_db_manager: Arc<CacheDbManager>,
        replication_state: Arc<RwLock<ReplicationState>>,
        last_processed_id: i64,
        batch_size: usize,
    ) -> Result<usize> {
        // Fetch oplogs from PostgreSQL
        let oplogs = Self::fetch_oplogs(&db_manager, last_processed_id, batch_size).await?;

        if oplogs.is_empty() {
            return Ok(0);
        }

        debug!("Fetched {} oplogs to process", oplogs.len());

        // Group oplogs by entity
        let mut grouped_oplogs: HashMap<String, Vec<OplogRecord>> = HashMap::new();
        for oplog in oplogs {
            grouped_oplogs
                .entry(oplog.entity.clone())
                .or_default()
                .push(oplog);
        }

        let mut total_processed = 0;
        let mut max_processed_id = last_processed_id;

        // Process each entity's oplogs
        for (entity_name, entity_oplogs) in grouped_oplogs {
            // Update max processed ID first (before potentially moving entity_oplogs)
            for oplog in &entity_oplogs {
                max_processed_id = max_processed_id.max(oplog.id);
            }

            // Check if entity is live (only process oplogs for live entities)
            match entity_manager.get_entity_status(&entity_name).await? {
                Some(EntityStatus::Live) => {
                    let processed = Self::apply_entity_oplogs(
                        &cache_db_manager,
                        &entity_manager,
                        &entity_name,
                        entity_oplogs,
                    )
                    .await?;

                    total_processed += processed;
                }
                Some(status) => {
                    debug!(
                        "Skipping oplogs for entity {} with status {:?}",
                        entity_name, status
                    );
                    // Max processed ID already updated above
                }
                None => {
                    warn!("Unknown entity {}, skipping oplogs", entity_name);
                    // Max processed ID already updated above
                }
            }
        }

        // Update replication state
        {
            let mut state = replication_state.write().await;
            state.last_processed_id = max_processed_id;
            state.total_processed += total_processed as u64;
            state.last_processed_at = Some(Instant::now());
        }

        // Save state to metadata database
        Self::save_replication_state(&db_manager, max_processed_id).await?;

        Ok(total_processed)
    }

    /// Fetch oplogs from PostgreSQL
    async fn fetch_oplogs(
        db_manager: &DatabaseManager,
        last_processed_id: i64,
        limit: usize,
    ) -> Result<Vec<OplogRecord>> {
        let rows = sqlx::query(
            "SELECT id, operation, doc_id, entity, data, version
             FROM oplog
             WHERE id > $1
             ORDER BY id ASC
             LIMIT $2",
        )
        .bind(last_processed_id)
        .bind(limit as i64)
        .fetch_all(db_manager.postgres())
        .await?;

        let mut oplogs = Vec::new();
        for row in rows {
            let data_str: String = row.get("data");
            let oplog = OplogRecord {
                id: row.get("id"),
                operation: row.get::<String, _>("operation").parse()?,
                doc_id: row.get("doc_id"),
                entity: row.get("entity"),
                data: serde_json::from_str(&data_str)?,
                version: row.get("version"),
            };
            oplogs.push(oplog);
        }

        Ok(oplogs)
    }

    /// Apply oplogs for a specific entity to its cache database
    async fn apply_entity_oplogs(
        cache_db_manager: &CacheDbManager,
        entity_manager: &EntityManager,
        entity_name: &str,
        oplogs: Vec<OplogRecord>,
    ) -> Result<usize> {
        debug!(
            "Applying {} oplogs for entity {}",
            oplogs.len(),
            entity_name
        );

        // Get entity configuration
        let entity = entity_manager
            .get_entity(entity_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Entity not found: {}", entity_name))?;

        // Get SQLite connection for this entity
        let sqlite_pool = cache_db_manager.get_or_create_pool(entity_name).await?;

        // Begin transaction
        let mut tx = sqlite_pool.begin().await?;

        let mut processed_count = 0;

        for oplog in oplogs {
            match oplog.operation {
                Operation::Upsert => {
                    Self::apply_upsert(&mut tx, &entity, &oplog).await?;
                }
                Operation::Delete => {
                    Self::apply_delete(&mut tx, &entity, &oplog).await?;
                }
            }
            processed_count += 1;
        }

        // Commit transaction
        tx.commit().await?;

        debug!(
            "Successfully applied {} oplogs for entity {}",
            processed_count, entity_name
        );
        Ok(processed_count)
    }

    /// Apply upsert operation to SQLite cache
    async fn apply_upsert(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        entity: &Entity,
        oplog: &OplogRecord,
    ) -> Result<()> {
        // For simplicity, we'll create a generic table structure
        // In a real implementation, you'd use the entity shape to create proper columns

        // Ensure table exists
        Self::ensure_cache_table_exists(tx, &entity.name).await?;

        // Upsert the document
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO cache_data (doc_id, data, version, updated_at)
            VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)
            "#,
        )
        .bind(&oplog.doc_id)
        .bind(serde_json::to_string(&oplog.data)?)
        .bind(oplog.version)
        .execute(&mut **tx)
        .await?;

        debug!(
            "Applied upsert for doc_id {} in entity {}",
            oplog.doc_id, entity.name
        );
        Ok(())
    }

    /// Apply delete operation to SQLite cache
    async fn apply_delete(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        entity: &Entity,
        oplog: &OplogRecord,
    ) -> Result<()> {
        // Ensure table exists
        Self::ensure_cache_table_exists(tx, &entity.name).await?;

        // Delete the document
        let rows_affected = sqlx::query("DELETE FROM cache_data WHERE doc_id = ?1")
            .bind(&oplog.doc_id)
            .execute(&mut **tx)
            .await?
            .rows_affected();

        debug!(
            "Applied delete for doc_id {} in entity {} (rows affected: {})",
            oplog.doc_id, entity.name, rows_affected
        );
        Ok(())
    }

    /// Ensure cache table exists in SQLite database
    async fn ensure_cache_table_exists(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        _entity_name: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cache_data (
                doc_id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                version INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&mut **tx)
        .await?;

        // Create index for better performance
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cache_data_version ON cache_data(version)")
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    /// Load replication state from metadata database
    async fn load_replication_state(db_manager: &DatabaseManager) -> Result<ReplicationState> {
        let last_processed_id = match db_manager.get_metadata("cache_replication_offset").await? {
            Some(value) => value.parse().unwrap_or(0),
            None => 0,
        };

        Ok(ReplicationState {
            last_processed_id,
            entity_offsets: HashMap::new(),
            total_processed: 0,
            failed_operations: 0,
            last_processed_at: None,
        })
    }

    /// Save replication state to metadata database
    async fn save_replication_state(
        db_manager: &DatabaseManager,
        last_processed_id: i64,
    ) -> Result<()> {
        db_manager
            .set_metadata("cache_replication_offset", &last_processed_id.to_string())
            .await?;
        Ok(())
    }

    /// Get replication statistics
    pub async fn get_stats(&self) -> Result<ReplicationStats> {
        let state = self.replication_state.read().await;

        // Get total oplogs count from PostgreSQL
        let total_oplogs = self.get_total_oplog_count().await?;
        let pending_oplogs = (total_oplogs - state.last_processed_id).max(0);

        Ok(ReplicationStats {
            last_processed_id: state.last_processed_id,
            total_oplogs,
            pending_oplogs,
            processed_oplogs: state.total_processed as i64,
            failed_operations: state.failed_operations as i64,
        })
    }

    /// Get total oplog count from PostgreSQL
    async fn get_total_oplog_count(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(id), 0) as max_id FROM oplog")
            .fetch_one(self.db_manager.postgres())
            .await?;

        Ok(row.get("max_id"))
    }

    /// Recreate cache database for an entity (for corruption recovery)
    pub async fn recreate_entity_cache(&self, entity_name: &str) -> Result<()> {
        info!("Recreating cache database for entity: {}", entity_name);

        // Remove existing database
        self.cache_db_manager
            .remove_entity_database(entity_name)
            .await?;

        // Get entity configuration
        let _entity = self
            .entity_manager
            .get_entity(entity_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Entity not found: {}", entity_name))?;

        // Create new database and apply all oplogs
        let _sqlite_pool = self
            .cache_db_manager
            .get_or_create_pool(entity_name)
            .await?;

        // Get all oplogs for this entity
        let oplogs = self.fetch_entity_oplogs(entity_name).await?;

        info!(
            "Applying {} oplogs to recreated cache for entity {}",
            oplogs.len(),
            entity_name
        );

        // Apply all oplogs
        Self::apply_entity_oplogs(
            &self.cache_db_manager,
            &self.entity_manager,
            entity_name,
            oplogs,
        )
        .await?;

        info!(
            "Successfully recreated cache database for entity: {}",
            entity_name
        );
        Ok(())
    }

    /// Fetch all oplogs for a specific entity
    async fn fetch_entity_oplogs(&self, entity_name: &str) -> Result<Vec<OplogRecord>> {
        let rows = sqlx::query(
            "SELECT id, operation, doc_id, entity, data, version
             FROM oplog
             WHERE entity = $1
             ORDER BY id ASC",
        )
        .bind(entity_name)
        .fetch_all(self.db_manager.postgres())
        .await?;

        let mut oplogs = Vec::new();
        for row in rows {
            let data_str: String = row.get("data");
            let oplog = OplogRecord {
                id: row.get("id"),
                operation: row.get::<String, _>("operation").parse()?,
                doc_id: row.get("doc_id"),
                entity: row.get("entity"),
                data: serde_json::from_str(&data_str)?,
                version: row.get("version"),
            };
            oplogs.push(oplog);
        }

        Ok(oplogs)
    }
}

#[async_trait::async_trait]
impl Job for CacheReplicator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self) -> Result<()> {
        debug!("Cache replicator job execution - checking replication health");

        // Perform health checks and maintenance
        let stats = self.get_stats().await?;

        if stats.pending_oplogs > 10000 {
            warn!("High number of pending oplogs: {}", stats.pending_oplogs);
        }

        if stats.failed_operations > 100 {
            warn!(
                "High number of failed operations: {}",
                stats.failed_operations
            );
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down cache replicator");

        // Save final state
        let state = self.replication_state.read().await;
        Self::save_replication_state(&self.db_manager, state.last_processed_id).await?;

        Ok(())
    }
}

/// Oplog record from database
#[derive(Debug)]
struct OplogRecord {
    id: i64,
    operation: Operation,
    doc_id: String,
    entity: String,
    data: JsonValue,
    version: i32,
}

/// Replication statistics
#[derive(Debug, serde::Serialize)]
pub struct ReplicationStats {
    pub last_processed_id: i64,
    pub total_oplogs: i64,
    pub pending_oplogs: i64,
    pub processed_oplogs: i64,
    pub failed_operations: i64,
}
