use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use rusqlite::{Connection, ToSql, Transaction, backup::Backup, params};
use tracing::debug;

use crate::replicator::{
    mapper::generate_insert_query,
    migrator::{DATA_TABLE, METADATA_TABLE},
};
use crate::{
    replicator::migrator::run_migrations,
    types::{Entity, Operation, Oplog},
};

const MAX_OPLOG_ID_KEY: &str = "max_oplog_id";

#[derive(Debug, Clone)]
pub struct LocalCache {
    db: Arc<Mutex<Connection>>,
    entity: Entity,
    max_oplog_id: Arc<AtomicU64>,
}

impl LocalCache {
    pub fn new(db: Arc<Mutex<Connection>>, entity: Entity) -> Self {
        Self {
            db,
            entity,
            max_oplog_id: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        let conn = self.db.lock().unwrap();
        run_migrations(&conn, &self.entity.shape)?;
        drop(conn);

        let max_oplog_id = self.load_max_oplog_id()?;
        self.max_oplog_id.store(max_oplog_id, Ordering::Relaxed);
        Ok(())
    }

    fn load_max_oplog_id(&self) -> anyhow::Result<u64> {
        let conn = self.db.lock().unwrap();
        let query = format!("SELECT value FROM {METADATA_TABLE} WHERE key = ?");

        let mut stmt = conn.prepare(&query)?;
        let result: Result<u64, rusqlite::Error> = stmt.query_row(params![MAX_OPLOG_ID_KEY], |row| row.get(0));

        match result {
            Ok(value) => Ok(value),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn apply_oplogs(&self, oplogs: Vec<Oplog>) -> anyhow::Result<()> {
        self.apply_entity_oplogs(oplogs)
    }

    fn apply_entity_oplogs(&self, oplogs: Vec<Oplog>) -> anyhow::Result<()> {
        debug!("Applying {} oplogs for entity {}", oplogs.len(), self.entity.name);

        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction()?;

        let mut processed_count = 0;
        let mut max_processed_id = 0;

        for oplog in oplogs {
            match oplog.operation {
                Operation::Upsert => {
                    Self::apply_upsert(&tx, &self.entity, &oplog)?;
                }
                Operation::Delete => {
                    Self::apply_delete(&tx, &self.entity, &oplog)?;
                }
                Operation::SyncStart => {}
                Operation::SyncEnd => {}
            }
            processed_count += 1;
            max_processed_id = max_processed_id.max(oplog.id as u64);
        }

        self.set_max_oplog_id(&tx, max_processed_id)?;
        tx.commit().context("Failed to commit transaction")?;

        debug!(
            "Successfully applied {} oplogs for entity {}",
            processed_count, self.entity.name
        );

        self.max_oplog_id.store(max_processed_id, Ordering::Relaxed);

        Ok(())
    }

    fn apply_upsert(tx: &Transaction, entity: &Entity, oplog: &Oplog) -> anyhow::Result<()> {
        let (query, values) = generate_insert_query(entity, oplog)?;
        let params = values.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>();

        tx.execute(&query, params.as_slice())
            .map_err(|e| anyhow::anyhow!("Failed to apply upsert: {}, query: {}", e, query))?;

        debug!("Applied upsert for doc_id {} in entity {}", oplog.doc_id, entity.name);
        Ok(())
    }

    fn apply_delete(tx: &Transaction, entity: &Entity, oplog: &Oplog) -> anyhow::Result<()> {
        let query = format!("DELETE FROM {DATA_TABLE} WHERE id = ?1");
        let rows_affected = tx
            .execute(&query, params![&oplog.doc_id])
            .context("Failed to apply delete")?;

        debug!(
            "Applied delete for doc_id {} in entity {} (rows affected: {})",
            oplog.doc_id, entity.name, rows_affected
        );
        Ok(())
    }

    fn set_max_oplog_id(&self, tx: &Transaction, max_oplog_id: u64) -> anyhow::Result<()> {
        let query = format!(
            "INSERT INTO {METADATA_TABLE} (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        );

        tx.execute(&query, params![MAX_OPLOG_ID_KEY, max_oplog_id])
            .context("Failed to set last processed id")?;
        Ok(())
    }

    pub fn snapshot_to(&self, snapshot_path: &Path) -> anyhow::Result<u64> {
        let conn = self.db.lock().unwrap();
        let mut dst = Connection::open(snapshot_path)?;
        let backup = Backup::new(&conn, &mut dst)?;
        backup.run_to_completion(1000, Duration::from_micros(1), None)?;

        Ok(self.max_oplog_id.load(Ordering::Relaxed))
    }

    pub fn max_oplog_id(&self) -> u64 {
        self.max_oplog_id.load(Ordering::Relaxed)
    }
}
