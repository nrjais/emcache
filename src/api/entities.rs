use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tracing::error;
use validator::Validate;

use super::AppState;
use crate::types::{Column, DataType, Entity, IdColumn, IdType, Index, Shape};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/entities", get(get_entities))
        .route("/entities", post(create_entity))
        .route("/entities/{name}", get(get_entity))
        .route("/entities/{name}", delete(delete_entity))
}

async fn get_entities(State(state): State<AppState>) -> Result<Json<Vec<Entity>>, (StatusCode, Json<JsonValue>)> {
    Ok(Json(state.entity_manager.get_all_entities()))
}

async fn create_entity(
    State(state): State<AppState>,
    Json(request): Json<CreateEntityRequest>,
) -> Result<Json<Entity>, (StatusCode, Json<JsonValue>)> {
    let entity = Entity {
        id: 0,
        name: request.name,
        client: request.client,
        source: request.source,
        shape: request.shape.into(),
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

async fn get_entity(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Entity>, (StatusCode, Json<JsonValue>)> {
    match state.entity_manager.get_entity(&name) {
        Some(entity) => Ok(Json(entity)),
        None => Err((StatusCode::NOT_FOUND, Json(json!({ "error": "Entity not found" })))),
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct CreateEntityRequest {
    #[validate(length(min = 1))]
    pub name: String,
    #[validate(length(min = 1))]
    pub client: String,
    #[validate(length(min = 1))]
    pub source: String,
    #[validate(nested)]
    pub shape: ShapeRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct IdColumnRequest {
    #[validate(length(min = 1))]
    pub path: String,
    #[serde(rename = "type")]
    pub typ: IdType,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ColumnRequest {
    #[validate(length(min = 1))]
    pub name: String,
    #[serde(rename = "type")]
    pub typ: DataType,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct IndexRequest {
    #[validate(length(min = 1))]
    pub name: String,
    #[validate(length(min = 1))]
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ShapeRequest {
    #[validate(nested)]
    pub id_column: IdColumnRequest,
    #[validate(length(min = 1))]
    #[validate(nested)]
    pub columns: Vec<ColumnRequest>,
    #[validate(nested)]
    pub indexes: Vec<IndexRequest>,
}

impl From<ShapeRequest> for Shape {
    fn from(request: ShapeRequest) -> Self {
        Shape {
            id_column: request.id_column.into(),
            columns: request.columns.into_iter().map(|c| c.into()).collect(),
            indexes: request.indexes.into_iter().map(|i| i.into()).collect(),
        }
    }
}

impl From<ColumnRequest> for Column {
    fn from(request: ColumnRequest) -> Self {
        Column {
            name: request.name,
            typ: request.typ,
            path: request.path,
        }
    }
}

impl From<IdColumnRequest> for IdColumn {
    fn from(request: IdColumnRequest) -> Self {
        IdColumn {
            path: request.path,
            typ: request.typ,
        }
    }
}

impl From<IndexRequest> for Index {
    fn from(request: IndexRequest) -> Self {
        Index {
            name: request.name,
            columns: request.columns,
        }
    }
}
