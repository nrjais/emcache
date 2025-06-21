use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::cache_db_manager::CacheDbManager;
use crate::entity_manager::EntityManager;
use crate::job_server::Job;
use crate::types::EntityStatus;

/// Job for cleaning up unused entity databases
pub struct DatabaseCleanupJob {
    name: String,
    cache_db_manager: Arc<CacheDbManager>,
    entity_manager: Arc<EntityManager>,
}

impl DatabaseCleanupJob {
    /// Create a new database cleanup job
    pub fn new(cache_db_manager: Arc<CacheDbManager>, entity_manager: Arc<EntityManager>) -> Self {
        Self {
            name: "database_cleanup".to_string(),
            cache_db_manager,
            entity_manager,
        }
    }

    /// Get list of active entity names
    async fn get_active_entities(&self) -> Result<Vec<String>> {
        let all_entities = self.entity_manager.get_all_entities().await?;
        let active_entities: Vec<String> = all_entities
            .into_iter()
            .filter(|entity| entity.status == EntityStatus::Live)
            .map(|entity| entity.name)
            .collect();

        Ok(active_entities)
    }

    /// Perform cleanup operations
    async fn perform_cleanup(&self) -> Result<()> {
        info!("Starting database cleanup operations");

        // Get active entities
        let active_entities = self.get_active_entities().await?;
        info!("Found {} active entities", active_entities.len());

        // Cleanup unused entity databases
        if let Err(e) = self
            .cache_db_manager
            .cleanup_unused_entity_databases(&active_entities)
            .await
        {
            error!("Failed to cleanup unused entity databases: {}", e);
        }

        // Cleanup old databases (older than 30 days)
        match self.cache_db_manager.cleanup_old_databases(30).await {
            Ok(cleaned_count) => {
                if cleaned_count > 0 {
                    info!("Cleaned up {} old databases", cleaned_count);
                }
            }
            Err(e) => {
                error!("Failed to cleanup old databases: {}", e);
            }
        }

        // Cleanup unused connections
        if let Err(e) = self.cache_db_manager.cleanup_unused_connections().await {
            warn!("Failed to cleanup unused connections: {}", e);
        }

        // Get and log disk usage statistics
        match self.cache_db_manager.get_disk_usage_stats().await {
            Ok(stats) => {
                info!(
                    "Disk usage: {} bytes across {} databases in {}",
                    stats.total_size_bytes, stats.database_count, stats.base_directory
                );

                // Log top 5 largest databases
                let mut entity_sizes: Vec<_> = stats.entity_sizes.into_iter().collect();
                entity_sizes.sort_by(|a, b| b.1.cmp(&a.1));

                for (entity, size) in entity_sizes.into_iter().take(5) {
                    debug!("Entity {} database size: {} bytes", entity, size);
                }
            }
            Err(e) => {
                warn!("Failed to get disk usage statistics: {}", e);
            }
        }

        info!("Database cleanup operations completed");
        Ok(())
    }
}

#[async_trait]
impl Job for DatabaseCleanupJob {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self) -> Result<()> {
        debug!("Executing database cleanup job");
        self.perform_cleanup().await
    }
}
