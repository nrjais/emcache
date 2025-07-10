use anyhow::{Context, Result};
use tracing::debug;

use crate::storage::PostgresClient;
use crate::types::Oplog;

#[derive(Clone)]
pub struct OplogDatabase {
    client: PostgresClient,
}

impl OplogDatabase {
    pub fn new(client: PostgresClient) -> Self {
        Self { client }
    }

    pub async fn insert_to_oplog(&self, oplogs: Vec<Oplog>) -> Result<()> {
        debug!("Inserting {} oplogs to main table", oplogs.len());

        let mut tx = self.client.postgres().begin().await?;

        for oplog in oplogs {
            sqlx::query(
                "INSERT INTO oplog (operation, doc_id, created_at, entity, data)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(oplog.operation.to_string())
            .bind(&oplog.doc_id)
            .bind(oplog.created_at)
            .bind(&oplog.entity)
            .bind(&oplog.data)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        debug!("Successfully inserted oplogs to main table");
        Ok(())
    }

    pub async fn get_oplogs_by_entity(&self, entities: &[String], from: i64, limit: i64) -> Result<Vec<Oplog>> {
        let oplogs = sqlx::query_as!(
            Oplog,
            "SELECT * FROM oplog WHERE id > $1 AND entity = ANY($2) ORDER BY id ASC LIMIT $3",
            from,
            entities,
            limit
        )
        .fetch_all(self.client.postgres())
        .await?;
        Ok(oplogs)
    }

    pub async fn get_oplogs(&self, cursor: i64, limit: i64) -> anyhow::Result<Vec<Oplog>> {
        let rows = sqlx::query_as!(
            Oplog,
            "SELECT * FROM oplog WHERE id > $1 ORDER BY id ASC LIMIT $2",
            cursor,
            limit
        )
        .fetch_all(self.client.postgres())
        .await
        .context("Failed to get oplogs")?;

        Ok(rows)
    }
}
