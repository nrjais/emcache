use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use sqlx::SqlitePool;
use tracing::debug;

use crate::replicator::migrator::{DATA_TABLE, METADATA_TABLE};
use crate::{
    replicator::migrator::run_migrations,
    types::{Entity, Operation, Oplog},
};

const LAST_PROCESSED_ID_KEY: &str = "last_processed_id";

#[derive(Debug, Clone)]
pub struct LocalCache {
    db: SqlitePool,
    entity: Entity,
    last_processed_id: Arc<AtomicI64>,
}

impl LocalCache {
    pub fn new(db: SqlitePool, entity: Entity) -> Self {
        Self {
            db,
            entity,
            last_processed_id: Arc::new(AtomicI64::new(0)),
        }
    }

    pub async fn close(&self) {
        self.db.close().await;
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        run_migrations(&self.db, &self.entity.shape).await?;
        let last_processed_id = self.load_last_processed_id().await?;
        self.last_processed_id.store(last_processed_id, Ordering::Relaxed);
        Ok(())
    }

    async fn load_last_processed_id(&self) -> anyhow::Result<i64> {
        let last_processed_id = sqlx::query_scalar(&format!("SELECT value FROM {METADATA_TABLE} WHERE key = ?"))
            .bind(LAST_PROCESSED_ID_KEY)
            .fetch_optional(&self.db)
            .await?;
        Ok(last_processed_id.unwrap_or(0))
    }

    pub async fn apply_oplogs(&self, oplogs: Vec<Oplog>) -> anyhow::Result<()> {
        self.apply_entity_oplogs(oplogs).await
    }

    async fn apply_entity_oplogs(&self, oplogs: Vec<Oplog>) -> anyhow::Result<()> {
        debug!("Applying {} oplogs for entity {}", oplogs.len(), self.entity.name);

        let mut tx = self.db.begin().await?;

        let mut processed_count = 0;
        let mut max_processed_id = 0;

        for oplog in oplogs {
            match oplog.operation {
                Operation::Upsert => {
                    Self::apply_upsert(&mut tx, &self.entity, &oplog).await?;
                }
                Operation::Delete => {
                    Self::apply_delete(&mut tx, &self.entity, &oplog).await?;
                }
            }
            processed_count += 1;
            max_processed_id = max_processed_id.max(oplog.id);
        }

        self.set_last_processed_id(&mut tx, max_processed_id).await?;
        tx.commit().await?;

        debug!(
            "Successfully applied {} oplogs for entity {}",
            processed_count, self.entity.name
        );

        self.last_processed_id.store(max_processed_id, Ordering::Relaxed);

        Ok(())
    }

    async fn apply_upsert(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        entity: &Entity,
        oplog: &Oplog,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "INSERT OR REPLACE INTO {DATA_TABLE} (id, data) VALUES (?1, ?2)"
        ))
        .bind(&oplog.doc_id)
        .bind(serde_json::to_string(&oplog.data)?)
        .bind(oplog.created_at)
        .execute(&mut **tx)
        .await?;

        debug!("Applied upsert for doc_id {} in entity {}", oplog.doc_id, entity.name);
        Ok(())
    }

    async fn apply_delete(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        entity: &Entity,
        oplog: &Oplog,
    ) -> anyhow::Result<()> {
        let rows_affected = sqlx::query(&format!("DELETE FROM {DATA_TABLE} WHERE id = ?1"))
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

    async fn set_last_processed_id(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        last_processed_id: i64,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {METADATA_TABLE} (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        ))
        .bind(LAST_PROCESSED_ID_KEY)
        .bind(last_processed_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
