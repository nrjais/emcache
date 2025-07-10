use rusqlite::Connection;
use tracing::debug;

use crate::types::{DataType, IdType, Shape};

pub const DATA_TABLE: &str = "data";
pub const METADATA_TABLE: &str = "metadata";

fn sqlite_type(typ: DataType) -> String {
    match typ {
        DataType::Jsonb => "JSONB".to_string(),
        DataType::Any => "TEXT".to_string(),
        DataType::Bool => "INTEGER".to_string(),
        DataType::Number => "REAL".to_string(),
        DataType::Integer => "INTEGER".to_string(),
        DataType::String => "TEXT".to_string(),
        DataType::Bytes => "BLOB".to_string(),
    }
}

fn sqlite_id_type(typ: IdType) -> String {
    match typ {
        IdType::String => "TEXT".to_string(),
        IdType::Number => "INTEGER".to_string(),
    }
}

fn quote_name(name: &str) -> String {
    format!("`{name}`")
}

pub fn run_migrations(conn: &Connection, shape: &Shape) -> anyhow::Result<()> {
    create_tables(conn, shape)?;
    create_indexes(conn, shape)?;

    debug!("SQLite migrations completed");
    Ok(())
}

fn create_tables(conn: &Connection, shape: &Shape) -> Result<(), anyhow::Error> {
    let column_defs = shape
        .columns
        .iter()
        .map(|c| format!("{} {}", quote_name(&c.name), sqlite_type(c.typ)))
        .collect::<Vec<String>>();
    let id = format!(
        "{} {} PRIMARY KEY",
        quote_name("id"),
        sqlite_id_type(shape.id_column.typ)
    );

    let columns = [vec![id], column_defs].concat().join(", ");

    let query = format!("CREATE TABLE IF NOT EXISTS {DATA_TABLE} ({columns})");
    conn.execute(&query, [])?;

    let meta = format!("CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (key TEXT PRIMARY KEY, value ANY NOT NULL) STRICT");
    conn.execute(&meta, [])?;

    Ok(())
}

fn create_indexes(conn: &Connection, shape: &Shape) -> anyhow::Result<()> {
    for index in &shape.indexes {
        let columns = index
            .columns
            .iter()
            .map(|c| quote_name(c))
            .collect::<Vec<String>>()
            .join(", ");

        let name = quote_name(&index.name);
        let query = format!("CREATE INDEX IF NOT EXISTS {name} ON {DATA_TABLE} ({columns})");
        conn.execute(&query, [])?;
    }

    Ok(())
}
