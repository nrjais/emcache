use rusqlite::Connection;
use tracing::debug;

use crate::types::Shape;

pub const DATA_TABLE: &str = "data";
pub const METADATA_TABLE: &str = "metadata";

pub fn run_migrations(conn: &Connection, _shape: &Shape) -> anyhow::Result<()> {
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, data TEXT NOT NULL) STRICT",
        DATA_TABLE
    );
    conn.execute(&query, [])?;

    let query = format!(
        "CREATE TABLE IF NOT EXISTS {} (key TEXT PRIMARY KEY, value ANY NOT NULL) STRICT",
        METADATA_TABLE
    );
    conn.execute(&query, [])?;

    debug!("SQLite migrations completed");
    Ok(())
}
