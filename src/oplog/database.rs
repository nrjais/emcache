use anyhow::Result;
use sqlx::Row;
use std::sync::Arc;
use tracing::{debug, info};

use crate::database::DatabaseManager;
use crate::types::Oplog;

/// Database operations for oplog management
#[derive(Clone)]
pub struct OplogDatabase {
    db_manager: Arc<DatabaseManager>,
}

impl OplogDatabase {
    /// Create a new oplog database operations handler
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        Self { db_manager }
    }

    /// Insert oplogs to staging table for scanning entities
    pub async fn insert_to_staging(&self, entity_name: &str, oplogs: Vec<Oplog>) -> Result<()> {
        debug!(
            "Inserting {} oplogs to staging for entity: {}",
            oplogs.len(),
            entity_name
        );

        let mut tx = self.db_manager.postgres().begin().await?;

        for oplog in oplogs {
            sqlx::query(
                "INSERT INTO oplog_staging (operation, doc_id, created_at, entity, data, version)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(oplog.operation.to_string())
            .bind(&oplog.doc_id)
            .bind(oplog.created_at)
            .bind(&oplog.entity)
            .bind(serde_json::to_string(&oplog.data)?)
            .bind(oplog.version)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        debug!(
            "Successfully inserted oplogs to staging for entity: {}",
            entity_name
        );
        Ok(())
    }

    /// Insert oplogs to main oplog table for live entities
    pub async fn insert_to_oplog(&self, entity_name: &str, oplogs: Vec<Oplog>) -> Result<()> {
        debug!(
            "Inserting {} oplogs to main table for entity: {}",
            oplogs.len(),
            entity_name
        );

        let mut tx = self.db_manager.postgres().begin().await?;

        for oplog in oplogs {
            sqlx::query(
                "INSERT INTO oplog (operation, doc_id, created_at, entity, data, version)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(oplog.operation.to_string())
            .bind(&oplog.doc_id)
            .bind(oplog.created_at)
            .bind(&oplog.entity)
            .bind(serde_json::to_string(&oplog.data)?)
            .bind(oplog.version)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        debug!(
            "Successfully inserted oplogs to main table for entity: {}",
            entity_name
        );
        Ok(())
    }

    /// Move staging oplogs to main table after scanning completes
    pub async fn promote_staging_to_main(&self, entity_name: &str) -> Result<()> {
        info!(
            "Promoting staging oplogs to main table for entity: {}",
            entity_name
        );

        let mut tx = self.db_manager.postgres().begin().await?;

        // Move from staging to main
        let rows_affected = sqlx::query(
            "INSERT INTO oplog (operation, doc_id, created_at, entity, data, version)
             SELECT operation, doc_id, created_at, entity, data, version
             FROM oplog_staging
             WHERE entity = $1",
        )
        .bind(entity_name)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        // Clean up staging table
        sqlx::query("DELETE FROM oplog_staging WHERE entity = $1")
            .bind(entity_name)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        info!(
            "Promoted {} oplogs from staging to main for entity: {}",
            rows_affected, entity_name
        );
        Ok(())
    }

    /// Get staging oplog count for an entity
    pub async fn get_staging_count(&self, entity_name: &str) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM oplog_staging WHERE entity = $1")
            .bind(entity_name)
            .fetch_one(self.db_manager.postgres())
            .await?;

        Ok(row.get::<i64, _>("count"))
    }

    /// Get main oplog count for an entity
    pub async fn get_oplog_count(&self, entity_name: &str) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM oplog WHERE entity = $1")
            .bind(entity_name)
            .fetch_one(self.db_manager.postgres())
            .await?;

        Ok(row.get::<i64, _>("count"))
    }

    /// Get total oplog count
    pub async fn get_total_oplog_count(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM oplog")
            .fetch_one(self.db_manager.postgres())
            .await?;

        Ok(row.get::<i64, _>("count"))
    }

    /// Get recent oplogs for monitoring
    pub async fn get_recent_oplogs(&self, limit: i64) -> Result<Vec<Oplog>> {
        let rows = sqlx::query(
            "SELECT id, operation, doc_id, created_at, entity, data, version
             FROM oplog
             ORDER BY created_at DESC
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(self.db_manager.postgres())
        .await?;

        let mut oplogs = Vec::new();
        for row in rows {
            let data_str: String = row.get("data");
            let oplog = Oplog {
                id: row.get("id"),
                operation: row.get::<String, _>("operation").parse()?,
                doc_id: row.get("doc_id"),
                created_at: row.get("created_at"),
                entity: row.get("entity"),
                data: serde_json::from_str(&data_str)?,
                version: row.get("version"),
            };
            oplogs.push(oplog);
        }

        Ok(oplogs)
    }

    /// Clean up old oplogs (optional maintenance operation)
    pub async fn cleanup_old_oplogs(&self, entity_name: &str, keep_days: i32) -> Result<u64> {
        let rows_affected = sqlx::query(
            "DELETE FROM oplog
             WHERE entity = $1
             AND created_at < NOW() - INTERVAL '$2 days'",
        )
        .bind(entity_name)
        .bind(keep_days)
        .execute(self.db_manager.postgres())
        .await?
        .rows_affected();

        info!(
            "Cleaned up {} old oplogs for entity: {}",
            rows_affected, entity_name
        );
        Ok(rows_affected)
    }
}
