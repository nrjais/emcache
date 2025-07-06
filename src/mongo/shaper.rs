use anyhow::{Context, bail};
use mongodb::{
    bson::{self, Bson},
    change_stream::event::{ChangeStreamEvent, OperationType},
};
use serde_json::Value;
use tracing::debug;

use crate::types::{Operation, Oplog, OplogEvent, OplogFrom};

pub fn map_oplog_from_change(
    event: ChangeStreamEvent<bson::Document>,
    entity: &str,
) -> anyhow::Result<Option<OplogEvent>> {
    let operation = match event.operation_type {
        OperationType::Insert | OperationType::Update | OperationType::Replace => Operation::Upsert,
        OperationType::Delete => Operation::Delete,
        _ => {
            debug!("Ignoring change event type: {:?}", event.operation_type);
            return Ok(None);
        }
    };

    let resume_token = serde_json::to_value(event.id)?;

    let Some(doc_key) = event.document_key else {
        debug!("Change event missing document_key");
        return Ok(None);
    };

    let doc_id = doc_key.get("_id").context("Document id is not present")?;
    let doc_id = extract_doc_id(doc_id)?;

    let data = event
        .full_document
        .map(serde_json::to_value)
        .transpose()?
        .unwrap_or(serde_json::Value::Null);

    Ok(Some(OplogEvent {
        oplog: Oplog {
            id: 0,
            operation,
            doc_id,
            entity: entity.to_string(),
            data,
            created_at: chrono::Utc::now(),
        },
        from: OplogFrom::Live,
        data: resume_token,
    }))
}

pub fn map_oplog_from_document(document: bson::Document, entity: &str) -> anyhow::Result<OplogEvent> {
    let doc_id = document.get("_id").context("Document id is not present")?;
    let doc_id = extract_doc_id(doc_id)?;

    let data = serde_json::to_value(document)?;

    Ok(OplogEvent {
        oplog: Oplog {
            id: 0,
            operation: Operation::Upsert,
            doc_id,
            entity: entity.to_string(),
            data,
            created_at: chrono::Utc::now(),
        },
        from: OplogFrom::Scan,
        data: Value::Null,
    })
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
