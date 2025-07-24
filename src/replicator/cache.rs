use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use rusqlite::{Connection, Error::QueryReturnedNoRows, ToSql, Transaction, backup::Backup, params};
use tracing::debug;

use crate::replicator::{
    mapper::generate_insert_query,
    migrator::{DATA_SYNC_TABLE, DATA_TABLE, METADATA_TABLE},
    mode::Mode,
};
use crate::{
    replicator::migrator::run_migrations,
    types::{Entity, Operation, Oplog},
};

const MAX_OPLOG_ID_KEY: &str = "max_oplog_id";
const MODE_KEY: &str = "mode";

#[derive(Debug, Clone)]
pub struct LocalCache {
    db: Arc<Mutex<Connection>>,
    entity: Entity,
    max_oplog_id: Arc<AtomicU64>,
    mode: Arc<Mutex<Mode>>,
}

impl LocalCache {
    pub fn new(db: Arc<Mutex<Connection>>, entity: Entity) -> Self {
        Self {
            db,
            entity,
            max_oplog_id: Arc::new(AtomicU64::new(0)),
            mode: Arc::new(Mutex::new(Mode::Live)),
        }
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        let conn = self.db.lock().unwrap();
        run_migrations(&conn, &self.entity.shape)?;
        drop(conn);

        let max_oplog_id = self.load_max_oplog_id()?;
        self.max_oplog_id.store(max_oplog_id, Ordering::Relaxed);

        let mode = self.load_mode()?;
        *self.mode.lock().unwrap() = mode;

        Ok(())
    }

    fn load_max_oplog_id(&self) -> anyhow::Result<u64> {
        let conn = self.db.lock().unwrap();
        let query = format!("SELECT value FROM {METADATA_TABLE} WHERE key = ?");

        let mut stmt = conn.prepare(&query)?;
        let result: Result<u64, rusqlite::Error> = stmt.query_row(params![MAX_OPLOG_ID_KEY], |row| row.get(0));

        match result {
            Ok(value) => Ok(value),
            Err(QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    fn load_mode(&self) -> anyhow::Result<Mode> {
        let conn = self.db.lock().unwrap();
        let query = format!("SELECT value FROM {METADATA_TABLE} WHERE key = ?");

        let mut stmt = conn.prepare(&query)?;
        let result: Result<String, rusqlite::Error> = stmt.query_row(params![MODE_KEY], |row| row.get(0));

        match result {
            Ok(value) => Ok(value.into()),
            Err(QueryReturnedNoRows) => Ok(Mode::Live),
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

        let mut mode = self.mode.lock().unwrap();
        let mut current_mode = *mode;

        for oplog in oplogs {
            match oplog.operation {
                Operation::Upsert => {
                    Self::apply_upsert(&tx, &self.entity, &oplog, current_mode)?;
                }
                Operation::Delete => {
                    Self::apply_delete(&tx, &self.entity, &oplog, current_mode)?;
                }
                Operation::SyncStart => {
                    Self::apply_sync_start(&tx, &self.entity)?;
                    current_mode = Mode::Sync;
                }
                Operation::SyncEnd => {
                    if current_mode == Mode::Sync {
                        Self::apply_sync_end(&tx, &self.entity)?;
                        current_mode = Mode::Live;
                    }
                }
            }
            processed_count += 1;
            max_processed_id = max_processed_id.max(oplog.id as u64);
        }

        self.set_max_oplog_id(&tx, max_processed_id)?;
        self.set_mode(&tx, current_mode)?;
        tx.commit().context("Failed to commit transaction")?;
        *mode = current_mode;

        debug!(
            "Successfully applied {} oplogs for entity {}, mode: {}",
            processed_count, self.entity.name, current_mode
        );

        self.max_oplog_id.store(max_processed_id, Ordering::Relaxed);

        Ok(())
    }

    fn apply_upsert(tx: &Transaction, entity: &Entity, oplog: &Oplog, mode: Mode) -> anyhow::Result<()> {
        let table_name = mode.table_name();

        let (query, values) = generate_insert_query(entity, oplog, table_name)?;
        let params = values.iter().map(|v| v as &dyn ToSql).collect::<Vec<_>>();

        tx.execute(&query, params.as_slice())
            .map_err(|e| anyhow::anyhow!("Failed to apply upsert: {}, query: {}", e, query))?;

        debug!(
            "Applied upsert for doc_id {} in entity {} to table {}",
            oplog.doc_id, entity.name, table_name
        );
        Ok(())
    }

    fn apply_delete(tx: &Transaction, entity: &Entity, oplog: &Oplog, mode: Mode) -> anyhow::Result<()> {
        let table_name = mode.table_name();

        let query = format!("DELETE FROM {table_name} WHERE id = ?1");
        let rows_affected = tx
            .execute(&query, params![&oplog.doc_id])
            .context("Failed to apply delete")?;

        debug!(
            "Applied delete for doc_id {} in entity {} from table {} (rows affected: {})",
            oplog.doc_id, entity.name, table_name, rows_affected
        );
        Ok(())
    }

    fn apply_sync_start(tx: &Transaction, entity: &Entity) -> anyhow::Result<()> {
        debug!("Starting sync for entity {}", entity.name);

        let truncate_sync_table_query = format!("DELETE FROM {DATA_SYNC_TABLE}");
        tx.execute(&truncate_sync_table_query, [])?;

        debug!("Successfully started sync for entity {}", entity.name);
        Ok(())
    }

    fn apply_sync_end(tx: &Transaction, entity: &Entity) -> anyhow::Result<()> {
        debug!("Ending sync for entity {}", entity.name);

        // Swap tables: data -> data_temp, data_sync -> data, data_temp -> data_sync
        let rename_data_to_temp_query = format!("ALTER TABLE {DATA_TABLE} RENAME TO data_temp");
        tx.execute(&rename_data_to_temp_query, [])?;

        let rename_sync_to_data_query = format!("ALTER TABLE {DATA_SYNC_TABLE} RENAME TO {DATA_TABLE}");
        tx.execute(&rename_sync_to_data_query, [])?;

        let rename_temp_to_sync_query = format!("ALTER TABLE data_temp RENAME TO {DATA_SYNC_TABLE}");
        tx.execute(&rename_temp_to_sync_query, [])?;

        let truncate_sync_table_query = format!("DELETE FROM {DATA_SYNC_TABLE}");
        tx.execute(&truncate_sync_table_query, [])?;

        debug!("Successfully ended sync for entity {}", entity.name);
        Ok(())
    }

    fn set_max_oplog_id(&self, tx: &Transaction, max_oplog_id: u64) -> anyhow::Result<()> {
        let query = self.metadata_insert_query();
        tx.execute(&query, params![MAX_OPLOG_ID_KEY, max_oplog_id])
            .context("Failed to set last processed id")?;
        Ok(())
    }

    fn set_mode(&self, tx: &Transaction, mode: Mode) -> anyhow::Result<()> {
        let query = self.metadata_insert_query();
        let mode_str: String = mode.into();
        tx.execute(&query, params![MODE_KEY, mode_str])
            .context("Failed to set mode")?;
        Ok(())
    }

    fn metadata_insert_query(&self) -> String {
        format!(
            "INSERT INTO {METADATA_TABLE} (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
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

    pub fn current_mode(&self) -> Mode {
        *self.mode.lock().unwrap()
    }
}
