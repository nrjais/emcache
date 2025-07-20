use std::default::Default;

use anyhow::{Context, bail};
use chrono::Utc;
use mongodb::{
    bson::{self, Bson},
    change_stream::event::{ChangeStreamEvent, OperationType},
};
use serde_json::Value;
use serde_json_path::JsonPath;
use thiserror::Error;
use tracing::{debug, error, warn};

use crate::types::{Entity, Operation, Oplog, OplogEvent, OplogFrom, Shape};

#[derive(Debug, Error)]
pub enum OplogError {
    #[error("Invalidate event")]
    Invalidate,
    #[error("Document error: {0}")]
    DocumentError(String),
    #[error("Failed to extract data for entity: {0}")]
    ExtractDataError(String),
}

fn extract_data(doc: &bson::Document, shape: &Shape) -> anyhow::Result<Vec<Value>> {
    let doc = serde_json::to_value(doc)?;
    let mut data = Vec::new();
    for column in &shape.columns {
        // Cache the parsed path
        let path = JsonPath::parse(&column.path).context(format!("Failed to parse path: {}", column.path))?;
        let value = path.query(&doc).first().cloned().unwrap_or(Value::Null);
        data.push(value);
    }

    Ok(data)
}

pub fn map_oplog_from_change(
    event: ChangeStreamEvent<bson::Document>,
    entity: &Entity,
) -> Result<Option<OplogEvent>, OplogError> {
    let operation = match event.operation_type {
        OperationType::Insert | OperationType::Update | OperationType::Replace => Operation::Upsert,
        OperationType::Delete => Operation::Delete,
        OperationType::Invalidate | OperationType::Rename | OperationType::Drop | OperationType::DropDatabase => {
            return Err(OplogError::Invalidate);
        }
        _ => {
            warn!("Ignoring change event type: {:?}", event.operation_type);
            return Ok(None);
        }
    };

    let resume_token = serde_json::to_value(event.id).map_err(|e| OplogError::DocumentError(e.to_string()))?;

    let Some(doc_key) = event.document_key else {
        debug!("Change event missing document_key");
        return Ok(None);
    };

    let doc_id = doc_key
        .get("_id")
        .context("Document id is not present")
        .map_err(|e| OplogError::DocumentError(e.to_string()))?;
    let doc_id = extract_doc_id(doc_id).map_err(|e| OplogError::DocumentError(e.to_string()))?;

    let data = event
        .full_document
        .map(|doc| extract_data(&doc, &entity.shape))
        .transpose()
        .map_err(|e| OplogError::ExtractDataError(e.to_string()))?
        .map(|data| Value::Array(data))
        .unwrap_or(Value::Null);

    Ok(Some(OplogEvent {
        oplog: Oplog {
            id: 0,
            operation,
            doc_id,
            entity: entity.name.clone(),
            data,
            created_at: Utc::now(),
        },
        from: OplogFrom::Live,
        data: resume_token,
    }))
}

pub fn map_oplog_from_document(document: bson::Document, entity: &Entity) -> Result<OplogEvent, OplogError> {
    let doc_id = document
        .get("_id")
        .context("Document id is not present")
        .map_err(|e| OplogError::DocumentError(e.to_string()))?;
    let doc_id = extract_doc_id(doc_id).map_err(|e| OplogError::DocumentError(e.to_string()))?;

    let data = extract_data(&document, &entity.shape).map_err(|e| OplogError::ExtractDataError(e.to_string()))?;

    Ok(OplogEvent {
        oplog: Oplog {
            id: 0,
            operation: Operation::Upsert,
            doc_id,
            entity: entity.name.clone(),
            data: Value::Array(data),
            created_at: Utc::now(),
        },
        from: OplogFrom::Scan,
        data: Value::Null,
    })
}

pub fn restart_sync_oplog(entity: String) -> OplogEvent {
    OplogEvent {
        oplog: Oplog {
            id: 0,
            operation: Operation::SyncStart,
            doc_id: Default::default(),
            entity,
            data: Value::Null,
            created_at: Utc::now(),
        },
        from: OplogFrom::Scan,
        data: Value::Null,
    }
}

pub fn end_sync_oplog(entity: String) -> OplogEvent {
    OplogEvent {
        oplog: Oplog {
            id: 0,
            operation: Operation::SyncEnd,
            doc_id: Default::default(),
            entity,
            data: Value::Null,
            created_at: Utc::now(),
        },
        from: OplogFrom::Scan,
        data: Value::Null,
    }
}

fn extract_doc_id(doc_id: &Bson) -> anyhow::Result<String> {
    let doc_id = match doc_id {
        Bson::ObjectId(id) => id.to_hex(),
        Bson::String(id) => id.clone(),
        Bson::Int32(id) => id.to_string(),
        Bson::Int64(id) => id.to_string(),
        _ => bail!("Document id is not valid identifier"),
    };

    Ok(doc_id)
}
