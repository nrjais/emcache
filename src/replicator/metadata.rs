use rusqlite::{Connection, params};
use std::sync::{Arc, Mutex};

const META_TABLE_NAME: &str = "meta";
const MAX_OPLOG_ID: &str = "max_oplog_id";

pub struct MetadataDb {
    db: Arc<Mutex<Connection>>,
}

impl MetadataDb {
    pub fn new(base_dir: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(format!("{base_dir}/metadata.db"))?;
        let db = Arc::new(Mutex::new(conn));

        let db = Self { db };
        db.init()?;

        Ok(db)
    }

    pub fn max_oplog_id(&self) -> anyhow::Result<i64> {
        let conn = self.db.lock().unwrap();
        let query = format!("SELECT value FROM {META_TABLE_NAME} WHERE key = ?");

        let mut stmt = conn.prepare(&query)?;
        let result: Result<i64, rusqlite::Error> = stmt.query_row(params![MAX_OPLOG_ID], |row| row.get(0));

        match result {
            Ok(value) => Ok(value),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_max_oplog_id(&self, max_oplog_id: i64) -> anyhow::Result<()> {
        let conn = self.db.lock().unwrap();
        let query = format!(
            "INSERT INTO {META_TABLE_NAME} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        );

        conn.execute(&query, params![MAX_OPLOG_ID, max_oplog_id])?;
        Ok(())
    }

    pub fn init(&self) -> anyhow::Result<()> {
        let conn = self.db.lock().unwrap();
        let query = format!("CREATE TABLE IF NOT EXISTS {META_TABLE_NAME} (key TEXT PRIMARY KEY, value ANY) STRICT");
        conn.execute(&query, [])?;
        Ok(())
    }
}
