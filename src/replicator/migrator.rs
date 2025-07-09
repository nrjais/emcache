use rusqlite::Connection;
use tracing::debug;

use crate::types::Shape;

pub const DATA_TABLE: &str = "data";
pub const METADATA_TABLE: &str = "metadata";

pub fn run_migrations(conn: &Connection, _shape: &Shape) -> anyhow::Result<()> {
    let query = format!("CREATE TABLE IF NOT EXISTS {DATA_TABLE} (id TEXT PRIMARY KEY, data TEXT NOT NULL) STRICT");
    conn.execute(&query, [])?;

    let query =
        format!("CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (key TEXT PRIMARY KEY, value ANY NOT NULL) STRICT");
    conn.execute(&query, [])?;

    debug!("SQLite migrations completed");
    Ok(())
}
