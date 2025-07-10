use anyhow::{Context, bail};
use chrono::Utc;
use jsonpath_rust::{parser::parse_json_path, query::js_path_process};
use mongodb::{
    bson::{self, Bson},
    change_stream::event::{ChangeStreamEvent, OperationType},
};
use serde_json::Value;
use tracing::{debug, error};

use crate::types::{Entity, Operation, Oplog, OplogEvent, OplogFrom, Shape};

fn extract_data(doc: &bson::Document, shape: &Shape) -> anyhow::Result<Vec<Value>> {
    let doc = serde_json::to_value(doc)?;
    let mut data = Vec::new();
    for column in &shape.columns {
        let path = parse_json_path(&column.path).context(format!("Failed to parse path: {}", column.path))?;
        let process = js_path_process(&path, &doc).context(format!("Failed to process path: {}", column.path))?;
        let first = process.into_iter().next();
        let value = first.map(|v| v.val().clone()).unwrap_or(Value::Null);
        data.push(value);
    }

    Ok(data)
}

pub fn map_oplog_from_change(
    event: ChangeStreamEvent<bson::Document>,
    entity: &Entity,
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
        .map(|doc| extract_data(&doc, &entity.shape))
        .transpose()
        .inspect_err(|e| error!("Failed to extract data for entity '{}': {}", entity.name, e))?
        .unwrap_or_default();

    Ok(Some(OplogEvent {
        oplog: Oplog {
            id: 0,
            operation,
            doc_id,
            entity: entity.name.clone(),
            data: Value::Array(data),
            created_at: Utc::now(),
        },
        from: OplogFrom::Live,
        data: resume_token,
    }))
}

pub fn map_oplog_from_document(document: bson::Document, entity: &Entity) -> anyhow::Result<OplogEvent> {
    let doc_id = document.get("_id").context("Document id is not present")?;
    let doc_id = extract_doc_id(doc_id)?;

    let data = extract_data(&document, &entity.shape)
        .context(format!("Failed to extract data for entity: {}", entity.name))?;

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
