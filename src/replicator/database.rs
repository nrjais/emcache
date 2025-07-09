use anyhow::Context;

use crate::{storage::PostgresClient, types::Oplog};

pub struct OplogDatabase {
    client: PostgresClient,
}

impl OplogDatabase {
    pub fn new(client: PostgresClient) -> Self {
        Self { client }
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
