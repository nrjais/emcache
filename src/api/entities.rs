use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tracing::error;

use super::AppState;
use crate::types::{Entity, Shape};

/// Create entity management router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/entities", get(get_entities))
        .route("/entities", post(create_entity))
        .route("/entities/{name}", get(get_entity))
        .route("/entities/{name}", delete(delete_entity))
}

/// Get all entities
async fn get_entities(State(state): State<AppState>) -> Result<Json<Vec<Entity>>, (StatusCode, Json<JsonValue>)> {
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
        id: 0,
        name: request.name,
        source: request.source,
        shape: request.shape,
        created_at: chrono::Utc::now(),
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
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(json!({ "error": "Entity not found" })))),
        Err(e) => {
            error!("Failed to get entity: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get entity" })),
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

// Request/Response types
#[derive(Deserialize)]
pub struct CreateEntityRequest {
    pub name: String,
    pub source: String,
    pub shape: Shape,
}
