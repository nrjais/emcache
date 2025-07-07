use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
};

use rusqlite::{Connection, params};
use tracing::debug;

use crate::replicator::migrator::{DATA_TABLE, METADATA_TABLE};
use crate::{
    replicator::migrator::run_migrations,
    types::{Entity, Operation, Oplog},
};

const LAST_PROCESSED_ID_KEY: &str = "last_processed_id";

#[derive(Debug, Clone)]
pub struct LocalCache {
    db: Arc<Mutex<Connection>>,
    entity: Entity,
    last_processed_id: Arc<AtomicI64>,
}

impl LocalCache {
    pub fn new(db: Arc<Mutex<Connection>>, entity: Entity) -> Self {
        Self {
            db,
            entity,
            last_processed_id: Arc::new(AtomicI64::new(0)),
        }
    }

    pub async fn close(&self) {
        // Connection will be closed when Arc is dropped
        debug!("Closing SQLite connection for entity: {}", self.entity.name);
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        let conn = self.db.lock().unwrap();
        run_migrations(&conn, &self.entity.shape)?;
        drop(conn);

        let last_processed_id = self.load_last_processed_id()?;
        self.last_processed_id.store(last_processed_id, Ordering::Relaxed);
        Ok(())
    }

    fn load_last_processed_id(&self) -> anyhow::Result<i64> {
        let conn = self.db.lock().unwrap();
        let query = format!("SELECT value FROM {} WHERE key = ?", METADATA_TABLE);

        let mut stmt = conn.prepare(&query)?;
        let result: Result<i64, rusqlite::Error> = stmt.query_row(params![LAST_PROCESSED_ID_KEY], |row| row.get(0));

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
            }
            processed_count += 1;
            max_processed_id = max_processed_id.max(oplog.id);
        }

        self.set_last_processed_id(&tx, max_processed_id)?;
        tx.commit()?;

        debug!(
            "Successfully applied {} oplogs for entity {}",
            processed_count, self.entity.name
        );

        self.last_processed_id.store(max_processed_id, Ordering::Relaxed);

        Ok(())
    }

    fn apply_upsert(tx: &rusqlite::Transaction, entity: &Entity, oplog: &Oplog) -> anyhow::Result<()> {
        let query = format!("INSERT OR REPLACE INTO {} (id, data) VALUES (?1, ?2)", DATA_TABLE);

        tx.execute(&query, params![&oplog.doc_id, &serde_json::to_string(&oplog.data)?,])?;

        debug!("Applied upsert for doc_id {} in entity {}", oplog.doc_id, entity.name);
        Ok(())
    }

    fn apply_delete(tx: &rusqlite::Transaction, entity: &Entity, oplog: &Oplog) -> anyhow::Result<()> {
        let query = format!("DELETE FROM {} WHERE id = ?1", DATA_TABLE);
        let rows_affected = tx.execute(&query, params![&oplog.doc_id])?;

        debug!(
            "Applied delete for doc_id {} in entity {} (rows affected: {})",
            oplog.doc_id, entity.name, rows_affected
        );
        Ok(())
    }

    fn set_last_processed_id(&self, tx: &rusqlite::Transaction, last_processed_id: i64) -> anyhow::Result<()> {
        let query = format!(
            "INSERT INTO {} (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            METADATA_TABLE
        );

        tx.execute(&query, params![LAST_PROCESSED_ID_KEY, last_processed_id])?;
        Ok(())
    }

    pub async fn snapshot_to(&self, snapshot_path: &Path) -> anyhow::Result<i64> {
        let conn = self.db.lock().unwrap();
        let backup_query = format!("VACUUM INTO ?1");

        conn.execute(&backup_query, params![snapshot_path.to_string_lossy()])?;

        debug!(
            "Created snapshot of entity {} at {}",
            self.entity.name,
            snapshot_path.display()
        );

        Ok(self.last_processed_id.load(Ordering::Relaxed))
    }
}
