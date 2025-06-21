use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post, put},
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tracing::error;

use super::AppState;
use crate::types::{Entity, EntityStatus, Shape};

/// Create entity management router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/entities", get(get_entities))
        .route("/entities", post(create_entity))
        .route("/entities/:name", get(get_entity))
        .route("/entities/:name", put(update_entity))
        .route("/entities/:name", delete(delete_entity))
        .route("/entities/:name/status", get(get_entity_status))
        .route("/entities/:name/status", put(update_entity_status))
        .route("/entities/stats", get(get_entities_stats))
        .route("/entities/bulk", post(create_entities_bulk))
}

/// Get all entities
async fn get_entities(
    State(state): State<AppState>,
) -> Result<Json<Vec<Entity>>, (StatusCode, Json<JsonValue>)> {
    match state.entity_manager.get_all_entities().await {
        Ok(entities) => Ok(Json(entities)),
        Err(e) => {
            error!("Failed to get entities: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get entities" })),
            ))
        }
    }
}

/// Create a new entity
async fn create_entity(
    State(state): State<AppState>,
    Json(request): Json<CreateEntityRequest>,
) -> Result<Json<Entity>, (StatusCode, Json<JsonValue>)> {
    let entity = Entity {
        id: 0, // Will be set by database
        name: request.name.clone(),
        shape: request.shape,
        status: EntityStatus::Pending,
        version: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    match state.entity_manager.create_entity(entity).await {
        Ok(created_entity) => Ok(Json(created_entity)),
        Err(e) => {
            error!("Failed to create entity: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to create entity" })),
            ))
        }
    }
}

/// Get a specific entity
async fn get_entity(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Entity>, (StatusCode, Json<JsonValue>)> {
    match state.entity_manager.get_entity(&name).await {
        Ok(Some(entity)) => Ok(Json(entity)),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Entity not found" })),
        )),
        Err(e) => {
            error!("Failed to get entity: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get entity" })),
            ))
        }
    }
}

/// Update an entity
async fn update_entity(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<UpdateEntityRequest>,
) -> Result<Json<Entity>, (StatusCode, Json<JsonValue>)> {
    // Get existing entity
    let mut entity = match state.entity_manager.get_entity(&name).await {
        Ok(Some(entity)) => entity,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Entity not found" })),
            ))
        }
        Err(e) => {
            error!("Failed to get entity: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get entity" })),
            ));
        }
    };

    // Update fields
    if let Some(shape) = request.shape {
        entity.shape = shape;
    }
    entity.updated_at = chrono::Utc::now();

    match state.entity_manager.update_entity(entity).await {
        Ok(updated_entity) => Ok(Json(updated_entity)),
        Err(e) => {
            error!("Failed to update entity: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to update entity" })),
            ))
        }
    }
}

/// Delete an entity
async fn delete_entity(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    match state.entity_manager.delete_entity(&name).await {
        Ok(()) => Ok(Json(json!({ "message": "Entity deleted successfully" }))),
        Err(e) => {
            error!("Failed to delete entity: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to delete entity" })),
            ))
        }
    }
}

/// Get entity status
async fn get_entity_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    match state.entity_manager.get_entity_status(&name).await {
        Ok(Some(status)) => Ok(Json(json!({ "status": status }))),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Entity not found" })),
        )),
        Err(e) => {
            error!("Failed to get entity status: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get entity status" })),
            ))
        }
    }
}

/// Update entity status
async fn update_entity_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<UpdateStatusRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    match state
        .entity_manager
        .update_entity_status(&name, request.status)
        .await
    {
        Ok(()) => Ok(Json(json!({ "message": "Status updated successfully" }))),
        Err(e) => {
            error!("Failed to update entity status: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to update entity status" })),
            ))
        }
    }
}

/// Get entity statistics
async fn get_entities_stats(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    match state.entity_manager.get_all_entities().await {
        Ok(entities) => {
            let mut status_counts = std::collections::HashMap::new();
            let mut version_counts = std::collections::HashMap::new();

            for entity in &entities {
                *status_counts.entry(entity.status.to_string()).or_insert(0) += 1;
                *version_counts.entry(entity.version).or_insert(0) += 1;
            }

            Ok(Json(json!({
                "total_entities": entities.len(),
                "status_distribution": status_counts,
                "version_distribution": version_counts,
                "latest_entities": entities.iter().take(5).map(|e| json!({
                    "name": e.name,
                    "status": e.status,
                    "version": e.version,
                    "created_at": e.created_at
                })).collect::<Vec<_>>(),
                "timestamp": chrono::Utc::now()
            })))
        }
        Err(e) => {
            error!("Failed to get entity statistics: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get entity statistics" })),
            ))
        }
    }
}

/// Create multiple entities in bulk
async fn create_entities_bulk(
    State(state): State<AppState>,
    Json(request): Json<BulkCreateRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    if request.entities.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "No entities specified for bulk creation" })),
        ));
    }

    if request.entities.len() > 100 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Too many entities (max 100 per request)" })),
        ));
    }

    let mut created_entities = Vec::new();
    let mut failed_entities = Vec::new();

    for entity_request in request.entities {
        let entity = Entity {
            id: 0, // Will be set by database
            name: entity_request.name.clone(),
            shape: entity_request.shape,
            status: EntityStatus::Pending,
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        match state.entity_manager.create_entity(entity).await {
            Ok(created_entity) => {
                created_entities.push(created_entity);
            }
            Err(e) => {
                failed_entities.push(json!({
                    "name": entity_request.name,
                    "error": e.to_string()
                }));
            }
        }
    }

    Ok(Json(json!({
        "created_count": created_entities.len(),
        "failed_count": failed_entities.len(),
        "created_entities": created_entities,
        "failed_entities": failed_entities,
        "timestamp": chrono::Utc::now()
    })))
}

// Request/Response types

#[derive(Deserialize)]
pub struct CreateEntityRequest {
    pub name: String,
    pub shape: Shape,
}

#[derive(Deserialize)]
pub struct UpdateEntityRequest {
    pub shape: Option<Shape>,
}

#[derive(Deserialize)]
pub struct UpdateStatusRequest {
    pub status: EntityStatus,
}

#[derive(Deserialize)]
pub struct BulkCreateRequest {
    pub entities: Vec<CreateEntityRequest>,
}
