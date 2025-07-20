use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::prelude::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Upsert,
    Delete,
    SyncStart,
    SyncEnd,
}

impl From<String> for Operation {
    fn from(s: String) -> Self {
        match s.as_str() {
            "upsert" => Operation::Upsert,
            "delete" => Operation::Delete,
            "sync_start" => Operation::SyncStart,
            "sync_end" => Operation::SyncEnd,
            _ => Operation::Upsert,
        }
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Upsert => write!(f, "upsert"),
            Operation::Delete => write!(f, "delete"),
            Operation::SyncStart => write!(f, "sync_start"),
            Operation::SyncEnd => write!(f, "sync_end"),
        }
    }
}

impl FromStr for Operation {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "upsert" => Ok(Operation::Upsert),
            "delete" => Ok(Operation::Delete),
            "sync_start" => Ok(Operation::SyncStart),
            "sync_end" => Ok(Operation::SyncEnd),
            _ => Err(anyhow::anyhow!("Invalid operation: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Jsonb,
    Any,
    Bool,
    Number,
    Integer,
    String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IdType {
    String,
    Number,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdColumn {
    pub path: String,
    #[serde(rename = "type")]
    pub typ: IdType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: DataType,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shape {
    pub id_column: IdColumn,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OplogFrom {
    Live,
    Scan,
}

#[derive(Debug, Clone)]
pub struct OplogEvent {
    pub oplog: Oplog,
    pub from: OplogFrom,
    pub data: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Oplog {
    pub id: i64,
    pub operation: Operation,
    pub doc_id: String,
    pub entity: String,
    pub data: JsonValue,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub client: String,
    pub source: String,
    pub shape: Shape,
    pub created_at: DateTime<Utc>,
}
