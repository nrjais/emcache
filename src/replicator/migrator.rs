use sqlx::SqlitePool;
use tracing::debug;

use crate::types::Shape;

pub const DATA_TABLE: &str = "data";
pub const METADATA_TABLE: &str = "metadata";

pub async fn run_migrations(pool: &SqlitePool, _shape: &Shape) -> anyhow::Result<()> {
    let query = format!("CREATE TABLE IF NOT EXISTS {DATA_TABLE} (id TEXT PRIMARY KEY, data TEXT NOT NULL) STRICT");
    sqlx::query(&query).execute(pool).await?;

    let query =
        format!("CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (key TEXT PRIMARY KEY, value ANY NOT NULL) STRICT");
    sqlx::query(&query).execute(pool).await?;

    debug!("SQLite migrations completed");
    Ok(())
}
