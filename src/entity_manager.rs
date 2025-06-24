use anyhow::Result;
use sqlx::Row;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::database::DatabaseManager;
use crate::job_server::Job;
use crate::types::{Entity, EntityStatus, Shape};

/// Entity manager handles entity CRUD operations and background refresh
pub struct EntityManager {
    db: Arc<DatabaseManager>,
}

impl EntityManager {
    /// Create a new entity manager
    pub fn new(db: Arc<DatabaseManager>) -> Self {
        Self { db }
    }

    /// Get entity by name
    pub async fn get_entity(&self, name: &str) -> Result<Option<Entity>> {
        debug!("Getting entity: {}", name);

        let result = sqlx::query(
            "SELECT id, name, entity_name, source_name, columns, status, version, created_at, updated_at
             FROM entities WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(self.db.postgres())
        .await?;

        match result {
            Some(row) => {
                let columns_json: String = row.get("columns");
                let columns = serde_json::from_str(&columns_json)?;

                let entity = Entity {
                    id: row.get("id"),
                    name: row.get("name"),
                    shape: Shape {
                        entity_name: row.get("entity_name"),
                        source_name: row.get("source_name"),
                        columns,
                    },
                    created_at: row.get("created_at"),
                };

                Ok(Some(entity))
            }
            None => Ok(None),
        }
    }

    /// Get all entities
    pub async fn get_all_entities(&self) -> Result<Vec<Entity>> {
        debug!("Getting all entities");

        let rows = sqlx::query(
            "SELECT id, name, entity_name, source_name, columns, status, version, created_at, updated_at
             FROM entities ORDER BY created_at DESC",
        )
        .fetch_all(self.db.postgres())
        .await?;

        let mut entities = Vec::new();
        for row in rows {
            let columns_json: String = row.get("columns");
            let columns = serde_json::from_str(&columns_json)?;

            let entity = Entity {
                id: row.get("id"),
                name: row.get("name"),
                shape: Shape {
                    entity_name: row.get("entity_name"),
                    source_name: row.get("source_name"),
                    columns,
                },
                created_at: row.get("created_at"),
            };

            entities.push(entity);
        }

        debug!("Found {} entities", entities.len());
        Ok(entities)
    }

    /// Update entity status
    pub async fn update_entity_status(&self, name: &str, status: EntityStatus) -> Result<()> {
        info!("Updating entity {} status to {:?}", name, status);

        let status_str = match status {
            EntityStatus::Pending => "pending",
            EntityStatus::Scanning => "scanning",
            EntityStatus::Live => "live",
            EntityStatus::Error => "error",
        };

        let rows_affected =
            sqlx::query("UPDATE entities SET status = $1, updated_at = NOW() WHERE name = $2")
                .bind(status_str)
                .bind(name)
                .execute(self.db.postgres())
                .await?
                .rows_affected();

        if rows_affected == 0 {
            return Err(anyhow::anyhow!("Entity not found: {}", name));
        }

        info!("Updated entity {} status to {}", name, status_str);
        Ok(())
    }

    /// Delete entity
    pub async fn delete_entity(&self, name: &str) -> Result<()> {
        info!("Deleting entity: {}", name);

        // Start a transaction to ensure atomicity
        let mut tx = self.db.postgres().begin().await?;

        // Delete from entities table
        let rows_affected = sqlx::query("DELETE FROM entities WHERE name = $1")
            .bind(name)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        if rows_affected == 0 {
            tx.rollback().await?;
            return Err(anyhow::anyhow!("Entity not found: {}", name));
        }

        // Clean up related oplog entries
        sqlx::query("DELETE FROM oplog WHERE entity = $1")
            .bind(name)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM oplog_staging WHERE entity = $1")
            .bind(name)
            .execute(&mut *tx)
            .await?;

        // Commit the transaction
        tx.commit().await?;

        // Clean up SQLite databases (async cleanup)
        if let Err(e) = self.db.remove_entity_database(name).await {
            warn!(
                "Failed to remove SQLite database for entity {}: {}",
                name, e
            );
            // Don't fail the delete operation if SQLite cleanup fails
        }

        info!("Entity deleted successfully: {}", name);
        Ok(())
    }

    /// Create entity with Entity struct
    pub async fn create_entity(&self, entity: Entity) -> Result<Entity> {
        info!("Creating entity from struct: {}", entity.name);

        let row = sqlx::query(
            r#"
            INSERT INTO entities (name, entity_name, source_name, columns, created_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
        )
        .bind(&entity.name)
        .bind(&entity.shape.entity_name)
        .bind(&entity.shape.source_name)
        .bind(serde_json::to_string(&entity.shape.columns)?)
        .bind(entity.created_at)
        .fetch_one(self.db.postgres())
        .await?;

        let mut created_entity = entity;
        created_entity.id = row.get("id");

        info!("Entity created successfully: {}", created_entity.name);
        Ok(created_entity)
    }

    /// Hard refresh all entities from database
    pub async fn hard_refresh(&self) -> Result<()> {
        info!("Performing hard refresh of entities");

        // Reload all entities and validate their configurations
        let entities = self.get_all_entities().await?;
        info!("Hard refresh found {} entities", entities.len());

        // Validate each entity
        for entity in &entities {
            if let Err(e) = self.validate_entity(entity).await {
                warn!("Entity validation failed for {}: {}", entity.name, e);
                // Set entity status to error if validation fails
                if let Err(update_err) = self
                    .update_entity_status(&entity.name, EntityStatus::Error)
                    .await
                {
                    error!(
                        "Failed to update entity {} status to error: {}",
                        entity.name, update_err
                    );
                }
            }
        }

        info!("Hard refresh and validation completed");
        Ok(())
    }

    /// Validate entity configuration
    async fn validate_entity(&self, entity: &Entity) -> Result<()> {
        if entity.shape.entity_name.is_empty() {
            return Err(anyhow::anyhow!("Entity name cannot be empty"));
        }

        if entity.shape.source_name.is_empty() {
            return Err(anyhow::anyhow!("Source name cannot be empty"));
        }

        if entity.shape.columns.is_empty() {
            return Err(anyhow::anyhow!("Entity must have at least one column"));
        }

        Ok(())
    }

    /// Refresh entities from database
    async fn refresh_entities(&self) -> Result<()> {
        debug!("Refreshing entities from database");

        // This is a placeholder for entity refresh logic
        // In a real implementation, this might:
        // 1. Check for new entities in the source system
        // 2. Update entity configurations
        // 3. Validate entity health

        let entities = self.get_all_entities().await?;
        info!("Refreshed {} entities", entities.len());

        Ok(())
    }
}

#[async_trait::async_trait]
impl Job for EntityManager {
    fn name(&self) -> &str {
        "entity_manager"
    }

    async fn execute(&self) -> Result<()> {
        debug!("Entity manager job execution");
        self.refresh_entities().await
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Entity manager shutdown");
        Ok(())
    }
}
