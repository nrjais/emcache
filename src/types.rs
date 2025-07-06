use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::prelude::FromRow;

/// Operation type for oplog entries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Upsert,
    Delete,
}

impl From<String> for Operation {
    fn from(s: String) -> Self {
        match s.as_str() {
            "upsert" => Operation::Upsert,
            "delete" => Operation::Delete,
            _ => Operation::Upsert,
        }
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Upsert => write!(f, "upsert"),
            Operation::Delete => write!(f, "delete"),
        }
    }
}

impl std::str::FromStr for Operation {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "upsert" => Ok(Operation::Upsert),
            "delete" => Ok(Operation::Delete),
            _ => Err(anyhow::anyhow!("Invalid operation: {}", s)),
        }
    }
}

/// Data type for entity columns
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Jsonb,
    Any,
    Bool,
    Number,
    Integer,
    String,
    Bytes,
}

/// Column definition for entity shapes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: DataType,
    pub path: String,
}

/// Entity shape definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shape {
    pub columns: Vec<Column>,
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

/// Oplog entry representing a change operation
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Oplog {
    pub id: i64,
    pub operation: Operation,
    pub doc_id: String,
    pub entity: String,
    pub data: JsonValue,
    pub created_at: DateTime<Utc>,
}

/// Entity definition stored in PostgreSQL
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub client: String,
    pub source: String,
    pub shape: Shape,
    pub created_at: DateTime<Utc>,
}

/// Resume token tracking for MongoDB change streams
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeToken {
    pub id: i64,
    pub token: String,
    pub version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Metadata entry for key-value storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub key: String,
    pub value: JsonValue,
}

/// Statistics about entity versions
#[derive(Debug, serde::Serialize)]
pub struct EntityVersionStats {
    pub total_entity_versions: u64,
    pub unique_entity_names: u64,
    pub live_versions: u64,
    pub archived_versions: u64,
    pub max_version: i32,
}
