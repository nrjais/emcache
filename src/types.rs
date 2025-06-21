use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Operation type for oplog entries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Upsert,
    Delete,
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
#[serde(rename_all = "UPPERCASE")]
pub enum DataType {
    #[serde(rename = "JSONB")]
    JsonB,
    Any,
    Bool,
    Number,
    Integer,
    Text,
}

/// Column definition for entity shapes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub r#type: DataType,
    pub path: String,
}

/// Entity shape definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shape {
    pub entity_name: String,
    pub source_name: String,
    pub columns: Vec<Column>,
}

/// Oplog entry representing a change operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Oplog {
    pub id: i64,
    pub operation: Operation,
    pub doc_id: String,
    pub created_at: DateTime<Utc>,
    pub entity: String,
    pub data: JsonValue,
    pub version: i32,
}

/// Entity status for tracking replication state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EntityStatus {
    Pending,
    Scanning,
    Live,
    Error,
}

impl std::fmt::Display for EntityStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityStatus::Pending => write!(f, "pending"),
            EntityStatus::Scanning => write!(f, "scanning"),
            EntityStatus::Live => write!(f, "live"),
            EntityStatus::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for EntityStatus {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(EntityStatus::Pending),
            "scanning" => Ok(EntityStatus::Scanning),
            "live" => Ok(EntityStatus::Live),
            "error" => Ok(EntityStatus::Error),
            _ => Err(anyhow::anyhow!("Invalid entity status: {}", s)),
        }
    }
}

/// Entity definition stored in PostgreSQL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub shape: Shape,
    pub status: EntityStatus,
    pub version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Resume token tracking for MongoDB change streams
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeToken {
    pub id: i64,
    pub token: String,
    pub version: i32,
    pub status: EntityStatus,
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
