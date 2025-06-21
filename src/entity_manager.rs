use anyhow::Result;
use chrono::Utc;
use sqlx::Row;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::database::DatabaseManager;
use crate::job_server::Job;
use crate::types::{Entity, EntityStatus, EntityVersionStats, Shape};

/// Entity manager handles entity CRUD operations and background refresh
pub struct EntityManager {
    name: String,
    db: Arc<DatabaseManager>,
}

impl EntityManager {
    /// Create a new entity manager
    pub fn new(db: Arc<DatabaseManager>) -> Self {
        Self {
            name: "entity_manager".to_string(),
            db,
        }
    }

    /// Create a new entity from name and shape
    pub async fn create_entity_from_shape(&self, name: String, shape: Shape) -> Result<Entity> {
        info!("Creating entity: {}", name);

        let now = Utc::now();
        let status_str = "pending";

        // Insert entity into database
        let row = sqlx::query(
            r#"
            INSERT INTO entities (name, entity_name, source_name, columns, status, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
        )
        .bind(&name)
        .bind(&shape.entity_name)
        .bind(&shape.source_name)
        .bind(serde_json::to_string(&shape.columns)?)
        .bind(status_str)
        .bind(now)
        .bind(now)
        .fetch_one(self.db.postgres())
        .await?;

        let entity = Entity {
            id: row.get("id"),
            name: name.clone(),
            shape,
            status: EntityStatus::Pending,
            version: 1,
            created_at: now,
            updated_at: now,
        };

        info!("Entity created successfully: {}", name);
        Ok(entity)
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

                let status_str: String = row.get("status");
                let status = match status_str.as_str() {
                    "pending" => EntityStatus::Pending,
                    "scanning" => EntityStatus::Scanning,
                    "live" => EntityStatus::Live,
                    "error" => EntityStatus::Error,
                    _ => EntityStatus::Error,
                };

                let entity = Entity {
                    id: row.get("id"),
                    name: row.get("name"),
                    shape: Shape {
                        entity_name: row.get("entity_name"),
                        source_name: row.get("source_name"),
                        columns,
                    },
                    status,
                    version: row.get("version"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
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

            let status_str: String = row.get("status");
            let status = match status_str.as_str() {
                "pending" => EntityStatus::Pending,
                "scanning" => EntityStatus::Scanning,
                "live" => EntityStatus::Live,
                "error" => EntityStatus::Error,
                _ => EntityStatus::Error,
            };

            let entity = Entity {
                id: row.get("id"),
                name: row.get("name"),
                shape: Shape {
                    entity_name: row.get("entity_name"),
                    source_name: row.get("source_name"),
                    columns,
                },
                status,
                version: row.get("version"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
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

    /// Get entity status by name
    pub async fn get_entity_status(&self, name: &str) -> Result<Option<EntityStatus>> {
        debug!("Getting entity status: {}", name);

        let result = sqlx::query("SELECT status FROM entities WHERE name = $1")
            .bind(name)
            .fetch_optional(self.db.postgres())
            .await?;

        match result {
            Some(row) => {
                let status_str: String = row.get("status");
                let status = match status_str.as_str() {
                    "pending" => EntityStatus::Pending,
                    "scanning" => EntityStatus::Scanning,
                    "live" => EntityStatus::Live,
                    "error" => EntityStatus::Error,
                    _ => EntityStatus::Error,
                };
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    /// Create entity with Entity struct
    pub async fn create_entity(&self, entity: Entity) -> Result<Entity> {
        info!("Creating entity from struct: {}", entity.name);

        let status_str = match entity.status {
            EntityStatus::Pending => "pending",
            EntityStatus::Scanning => "scanning",
            EntityStatus::Live => "live",
            EntityStatus::Error => "error",
        };

        let row = sqlx::query(
            r#"
            INSERT INTO entities (name, entity_name, source_name, columns, status, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
        )
        .bind(&entity.name)
        .bind(&entity.shape.entity_name)
        .bind(&entity.shape.source_name)
        .bind(serde_json::to_string(&entity.shape.columns)?)
        .bind(status_str)
        .bind(entity.created_at)
        .bind(entity.updated_at)
        .fetch_one(self.db.postgres())
        .await?;

        let mut created_entity = entity;
        created_entity.id = row.get("id");

        info!("Entity created successfully: {}", created_entity.name);
        Ok(created_entity)
    }

    /// Update entity
    pub async fn update_entity(&self, entity: Entity) -> Result<Entity> {
        info!("Updating entity: {}", entity.name);

        let status_str = match entity.status {
            EntityStatus::Pending => "pending",
            EntityStatus::Scanning => "scanning",
            EntityStatus::Live => "live",
            EntityStatus::Error => "error",
        };

        let rows_affected = sqlx::query(
            r#"
            UPDATE entities
            SET entity_name = $2, source_name = $3, columns = $4, status = $5, updated_at = $6
            WHERE name = $1
            "#,
        )
        .bind(&entity.name)
        .bind(&entity.shape.entity_name)
        .bind(&entity.shape.source_name)
        .bind(serde_json::to_string(&entity.shape.columns)?)
        .bind(status_str)
        .bind(entity.updated_at)
        .execute(self.db.postgres())
        .await?
        .rows_affected();

        if rows_affected == 0 {
            return Err(anyhow::anyhow!("Entity not found: {}", entity.name));
        }

        info!("Entity updated successfully: {}", entity.name);
        Ok(entity)
    }

    /// Get entities by status
    pub async fn get_entities_by_status(&self, status: EntityStatus) -> Result<Vec<Entity>> {
        debug!("Getting entities with status: {:?}", status);

        let status_str = match status {
            EntityStatus::Pending => "pending",
            EntityStatus::Scanning => "scanning",
            EntityStatus::Live => "live",
            EntityStatus::Error => "error",
        };

        let rows = sqlx::query(
            "SELECT id, name, entity_name, source_name, columns, status, version, created_at, updated_at
             FROM entities WHERE status = $1 ORDER BY created_at DESC",
        )
        .bind(status_str)
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
                status: status.clone(),
                version: row.get("version"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            };

            entities.push(entity);
        }

        debug!("Found {} entities with status {:?}", entities.len(), status);
        Ok(entities)
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

    /// Create or update entity with shape immutability (creates new version if shape changes)
    pub async fn create_or_update_entity_with_versioning(
        &self,
        name: String,
        shape: Shape,
    ) -> Result<Entity> {
        info!("Creating/updating entity with versioning: {}", name);

        // Check if entity already exists
        if let Ok(Some(existing_entity)) = self.get_entity(&name).await {
            // Compare shapes to see if they're different
            if self.shapes_are_different(&existing_entity.shape, &shape) {
                info!(
                    "Shape has changed for entity {}, creating new version",
                    name
                );

                // Archive the old entity version
                self.archive_entity_version(&existing_entity).await?;

                // Create new version with incremented version number
                let new_version = existing_entity.version + 1;
                let new_entity = self.create_entity_version(name, shape, new_version).await?;

                info!(
                    "Created new version {} for entity {}",
                    new_version, new_entity.name
                );

                return Ok(new_entity);
            } else {
                info!("Shape unchanged for entity {}, returning existing", name);
                return Ok(existing_entity);
            }
        }

        // Entity doesn't exist, create version 1
        info!("Creating new entity {} with version 1", name);
        self.create_entity_version(name, shape, 1).await
    }

    /// Create a specific version of an entity
    async fn create_entity_version(
        &self,
        name: String,
        shape: Shape,
        version: i32,
    ) -> Result<Entity> {
        let entity = Entity {
            id: 0, // Will be set by database
            name: name.clone(),
            shape,
            status: EntityStatus::Live,
            version,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        let query = r#"
            INSERT INTO entities (name, source_name, columns, status, version, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, name, source_name, columns, status, version, created_at, updated_at
        "#;

        let columns_json = serde_json::to_value(&entity.shape.columns)?;

        let row = sqlx::query(query)
            .bind(&entity.name)
            .bind(&entity.shape.source_name)
            .bind(&columns_json)
            .bind(entity.status.to_string())
            .bind(version)
            .bind(entity.created_at)
            .bind(entity.updated_at)
            .fetch_one(self.db.postgres())
            .await?;

        let created_entity = Entity {
            id: row.get("id"),
            name: row.get("name"),
            shape: Shape {
                entity_name: name.clone(),
                source_name: row.get("source_name"),
                columns: serde_json::from_value(row.get("columns"))?,
            },
            status: EntityStatus::from_str(row.get("status"))?,
            version: row.get("version"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        };

        info!(
            "Created entity version: {} v{}",
            created_entity.name, created_entity.version
        );
        Ok(created_entity)
    }

    /// Archive an entity version by setting its status to Archived
    async fn archive_entity_version(&self, entity: &Entity) -> Result<()> {
        info!(
            "Archiving entity version: {} v{}",
            entity.name, entity.version
        );

        let query = r#"
            UPDATE entities
            SET status = 'archived', updated_at = $1
            WHERE id = $2
        "#;

        sqlx::query(query)
            .bind(chrono::Utc::now())
            .bind(entity.id)
            .execute(self.db.postgres())
            .await?;

        info!(
            "Archived entity version: {} v{}",
            entity.name, entity.version
        );
        Ok(())
    }

    /// Compare two shapes to determine if they're different
    fn shapes_are_different(&self, shape1: &Shape, shape2: &Shape) -> bool {
        // Compare source names
        if shape1.source_name != shape2.source_name {
            return true;
        }

        // Compare number of columns
        if shape1.columns.len() != shape2.columns.len() {
            return true;
        }

        // Compare each column
        for (col1, col2) in shape1.columns.iter().zip(shape2.columns.iter()) {
            if col1.name != col2.name || col1.r#type != col2.r#type || col1.path != col2.path {
                return true;
            }
        }

        false
    }

    /// Get all versions of an entity (including archived)
    pub async fn get_entity_versions(&self, entity_name: &str) -> Result<Vec<Entity>> {
        info!("Fetching all versions for entity: {}", entity_name);

        let query = r#"
            SELECT id, name, source_name, columns, status, version, created_at, updated_at
            FROM entities
            WHERE name = $1
            ORDER BY version DESC
        "#;

        let rows = sqlx::query(query)
            .bind(entity_name)
            .fetch_all(self.db.postgres())
            .await?;

        let mut entities = Vec::new();
        for row in rows {
            let entity = Entity {
                id: row.get("id"),
                name: row.get("name"),
                shape: Shape {
                    entity_name: entity_name.to_string(),
                    source_name: row.get("source_name"),
                    columns: serde_json::from_value(row.get("columns"))?,
                },
                status: EntityStatus::from_str(row.get("status"))?,
                version: row.get("version"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            };
            entities.push(entity);
        }

        info!(
            "Found {} versions for entity {}",
            entities.len(),
            entity_name
        );
        Ok(entities)
    }

    /// Get the latest version of an entity (only live entities)
    pub async fn get_latest_entity_version(&self, entity_name: &str) -> Result<Entity> {
        info!("Fetching latest version for entity: {}", entity_name);

        let query = r#"
            SELECT id, name, source_name, columns, status, version, created_at, updated_at
            FROM entities
            WHERE name = $1 AND status = 'live'
            ORDER BY version DESC
            LIMIT 1
        "#;

        let row = sqlx::query(query)
            .bind(entity_name)
            .fetch_one(self.db.postgres())
            .await?;

        let entity = Entity {
            id: row.get("id"),
            name: row.get("name"),
            shape: Shape {
                entity_name: entity_name.to_string(),
                source_name: row.get("source_name"),
                columns: serde_json::from_value(row.get("columns"))?,
            },
            status: EntityStatus::from_str(row.get("status"))?,
            version: row.get("version"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        };

        info!(
            "Found latest version {} for entity {}",
            entity.version, entity_name
        );
        Ok(entity)
    }

    /// Cleanup archived entity versions older than specified days
    pub async fn cleanup_archived_versions(&self, older_than_days: i32) -> Result<u64> {
        info!(
            "Cleaning up archived entity versions older than {} days",
            older_than_days
        );

        let cutoff_date = chrono::Utc::now() - chrono::Duration::days(older_than_days as i64);

        let query = r#"
            DELETE FROM entities
            WHERE status = 'archived' AND updated_at < $1
        "#;

        let result = sqlx::query(query)
            .bind(cutoff_date)
            .execute(self.db.postgres())
            .await?;

        let deleted_count = result.rows_affected();
        info!("Cleaned up {} archived entity versions", deleted_count);
        Ok(deleted_count)
    }

    /// Get entity version statistics
    pub async fn get_version_statistics(&self) -> Result<EntityVersionStats> {
        info!("Fetching entity version statistics");

        let query = r#"
            SELECT
                COUNT(*) as total_entities,
                COUNT(DISTINCT name) as unique_entities,
                COUNT(CASE WHEN status = 'live' THEN 1 END) as live_versions,
                COUNT(CASE WHEN status = 'archived' THEN 1 END) as archived_versions,
                MAX(version) as max_version
            FROM entities
        "#;

        let row = sqlx::query(query).fetch_one(self.db.postgres()).await?;

        let stats = EntityVersionStats {
            total_entity_versions: row.get::<i64, _>("total_entities") as u64,
            unique_entity_names: row.get::<i64, _>("unique_entities") as u64,
            live_versions: row.get::<i64, _>("live_versions") as u64,
            archived_versions: row.get::<i64, _>("archived_versions") as u64,
            max_version: row.get::<Option<i32>, _>("max_version").unwrap_or(0),
        };

        info!("Version statistics: {:?}", stats);
        Ok(stats)
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
        &self.name
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
