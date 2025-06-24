use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use sqlx::Row;
use tracing::error;

use super::AppState;
use crate::types::Oplog;

/// Create oplog router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/oplogs", get(get_oplogs))
        .route("/oplogs/batch", post(get_batch_oplogs))
        .route("/oplogs/stats", get(get_oplog_stats))
        .route("/oplogs/entity/:entity", get(get_entity_oplogs))
}

/// Get oplogs with pagination
async fn get_oplogs(
    State(state): State<AppState>,
    Query(params): Query<OplogQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(1000); // Cap at 1000

    let mut query_str =
        "SELECT id, operation, doc_id, entity, data, version, created_at FROM oplog".to_string();
    let mut conditions = Vec::new();

    // Add entity filter if specified
    if let Some(entity) = &params.entity {
        conditions.push(format!("entity = '{entity}'"));
    }

    // Add since filter if specified
    if let Some(since) = &params.since {
        if let Ok(since_date) = chrono::DateTime::parse_from_rfc3339(since) {
            conditions.push(format!("created_at > '{}'", since_date.to_utc()));
        }
    }

    if !conditions.is_empty() {
        query_str.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
    }

    query_str.push_str(&format!(
        " ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    ));

    match sqlx::query(&query_str)
        .fetch_all(state.db_manager.postgres())
        .await
    {
        Ok(rows) => {
            let mut oplogs = Vec::new();
            for row in rows {
                let oplog = Oplog {
                    id: row.get("id"),
                    operation: serde_json::from_str(&format!(
                        "\"{}\"",
                        row.get::<String, _>("operation")
                    ))
                    .unwrap_or(crate::types::Operation::Upsert),
                    doc_id: row.get("doc_id"),
                    created_at: row.get("created_at"),
                    entity: row.get("entity"),
                    data: row.get("data"),
                };
                oplogs.push(oplog);
            }

            // Get total count for pagination
            let count_query = if conditions.is_empty() {
                "SELECT COUNT(*) as total FROM oplog".to_string()
            } else {
                format!(
                    "SELECT COUNT(*) as total FROM oplog WHERE {}",
                    conditions.join(" AND ")
                )
            };

            let total = sqlx::query(&count_query)
                .fetch_one(state.db_manager.postgres())
                .await
                .map(|row| row.get::<i64, _>("total"))
                .unwrap_or(0);

            Ok(Json(json!({
                "oplogs": oplogs,
                "total": total,
                "offset": offset,
                "limit": limit
            })))
        }
        Err(e) => {
            error!("Failed to get oplogs: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get oplogs" })),
            ))
        }
    }
}

/// Get batch of oplogs for multiple entities
async fn get_batch_oplogs(
    State(state): State<AppState>,
    Json(request): Json<BatchOplogRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    if request.entities.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "No entities specified" })),
        ));
    }

    let limit = request.limit.unwrap_or(100).min(1000);
    let entity_list = request
        .entities
        .iter()
        .map(|e| format!("'{e}'"))
        .collect::<Vec<_>>()
        .join(",");

    let mut query_str = format!(
        "SELECT id, operation, doc_id, entity, data, version, created_at FROM oplog WHERE entity IN ({entity_list})"
    );

    if let Some(since) = &request.since {
        if let Ok(since_date) = chrono::DateTime::parse_from_rfc3339(since) {
            query_str.push_str(&format!(" AND created_at > '{}'", since_date.to_utc()));
        }
    }

    query_str.push_str(&format!(" ORDER BY created_at DESC LIMIT {limit}"));

    match sqlx::query(&query_str)
        .fetch_all(state.db_manager.postgres())
        .await
    {
        Ok(rows) => {
            let mut oplogs_by_entity = std::collections::HashMap::new();

            for row in rows {
                let entity: String = row.get("entity");
                let oplog = Oplog {
                    id: row.get("id"),
                    operation: serde_json::from_str(&format!(
                        "\"{}\"",
                        row.get::<String, _>("operation")
                    ))
                    .unwrap_or(crate::types::Operation::Upsert),
                    doc_id: row.get("doc_id"),
                    created_at: row.get("created_at"),
                    entity: entity.clone(),
                    data: row.get("data"),
                };

                oplogs_by_entity
                    .entry(entity)
                    .or_insert_with(Vec::new)
                    .push(oplog);
            }

            Ok(Json(json!({
                "oplogs": oplogs_by_entity,
                "entities_requested": request.entities.len(),
                "entities_found": oplogs_by_entity.len()
            })))
        }
        Err(e) => {
            error!("Failed to get batch oplogs: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get batch oplogs" })),
            ))
        }
    }
}

/// Get oplogs for a specific entity
async fn get_entity_oplogs(
    State(state): State<AppState>,
    axum::extract::Path(entity): axum::extract::Path<String>,
    Query(params): Query<OplogQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(1000);

    let mut query_str = format!(
        "SELECT id, operation, doc_id, entity, data, version, created_at FROM oplog WHERE entity = '{entity}' "
    );

    if let Some(since) = &params.since {
        if let Ok(since_date) = chrono::DateTime::parse_from_rfc3339(since) {
            query_str.push_str(&format!(" AND created_at > '{}'", since_date.to_utc()));
        }
    }

    query_str.push_str(&format!(
        " ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    ));

    match sqlx::query(&query_str)
        .fetch_all(state.db_manager.postgres())
        .await
    {
        Ok(rows) => {
            let mut oplogs = Vec::new();
            for row in rows {
                let oplog = Oplog {
                    id: row.get("id"),
                    operation: serde_json::from_str(&format!(
                        "\"{}\"",
                        row.get::<String, _>("operation")
                    ))
                    .unwrap_or(crate::types::Operation::Upsert),
                    doc_id: row.get("doc_id"),
                    created_at: row.get("created_at"),
                    entity: row.get("entity"),
                    data: row.get("data"),
                };
                oplogs.push(oplog);
            }

            // Get total count for this entity
            let count_query = if params.since.is_some() {
                let since_date =
                    chrono::DateTime::parse_from_rfc3339(params.since.as_ref().unwrap())
                        .map(|d| d.to_utc())
                        .unwrap_or_else(|_| chrono::Utc::now());
                format!(
                    "SELECT COUNT(*) as total FROM oplog WHERE entity = '{entity}' AND created_at > '{since_date}'"
                )
            } else {
                format!(
                    "SELECT COUNT(*) as total FROM oplog WHERE entity = '{entity}'"
                )
            };

            let total = sqlx::query(&count_query)
                .fetch_one(state.db_manager.postgres())
                .await
                .map(|row| row.get::<i64, _>("total"))
                .unwrap_or(0);

            Ok(Json(json!({
                "entity": entity,
                "oplogs": oplogs,
                "total": total,
                "offset": offset,
                "limit": limit
            })))
        }
        Err(e) => {
            error!("Failed to get entity oplogs: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get entity oplogs" })),
            ))
        }
    }
}

/// Get oplog statistics
async fn get_oplog_stats(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // Get total oplogs
    let total_query = "SELECT COUNT(*) as total FROM oplog";
    let total_oplogs = sqlx::query(total_query)
        .fetch_one(state.db_manager.postgres())
        .await
        .map(|row| row.get::<i64, _>("total"))
        .unwrap_or(0);

    // Get oplogs by entity
    let entity_query =
        "SELECT entity, COUNT(*) as count FROM oplog GROUP BY entity ORDER BY count DESC LIMIT 10";
    let entity_counts = sqlx::query(entity_query)
        .fetch_all(state.db_manager.postgres())
        .await
        .unwrap_or_default();

    let mut oplogs_by_entity = std::collections::HashMap::new();
    for row in entity_counts {
        let entity: String = row.get("entity");
        let count: i64 = row.get("count");
        oplogs_by_entity.insert(entity, count);
    }

    // Get recent activity (last 10 oplogs)
    let recent_query =
        "SELECT entity, operation, created_at FROM oplog ORDER BY created_at DESC LIMIT 10";
    let recent_rows = sqlx::query(recent_query)
        .fetch_all(state.db_manager.postgres())
        .await
        .unwrap_or_default();

    let mut recent_activity = Vec::new();
    for row in recent_rows {
        recent_activity.push(json!({
            "entity": row.get::<String, _>("entity"),
            "operation": row.get::<String, _>("operation"),
            "created_at": row.get::<chrono::DateTime<chrono::Utc>, _>("created_at")
        }));
    }

    // Get staging oplogs count
    let staging_query = "SELECT COUNT(*) as total FROM oplog_staging";
    let staging_oplogs = sqlx::query(staging_query)
        .fetch_one(state.db_manager.postgres())
        .await
        .map(|row| row.get::<i64, _>("total"))
        .unwrap_or(0);

    Ok(Json(json!({
        "total_oplogs": total_oplogs,
        "staging_oplogs": staging_oplogs,
        "oplogs_by_entity": oplogs_by_entity,
        "recent_activity": recent_activity,
        "timestamp": chrono::Utc::now()
    })))
}

// Request types

#[derive(Deserialize)]
pub struct OplogQuery {
    pub entity: Option<String>,
    pub offset: Option<i64>,
    pub limit: Option<i64>,
    pub since: Option<String>,
}

#[derive(Deserialize)]
pub struct BatchOplogRequest {
    pub entities: Vec<String>,
    pub since: Option<String>,
    pub limit: Option<i64>,
}
