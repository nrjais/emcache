use std::collections::HashSet;

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use axum_extra::extract::WithRejection;
use garde::Validate;
use jsonpath_rust::parser::parse_json_path;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tracing::error;

use super::AppState;
use crate::{
    api::with_rejection::ApiError,
    config::Configs,
    types::{Column, DataType, Entity, IdColumn, IdType, Index, Shape},
};

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
    WithRejection(Json(request), _): WithRejection<Json<CreateEntityRequest>, ApiError>,
) -> Result<Json<Entity>, (StatusCode, Json<JsonValue>)> {
    if let Err(e) = request.clone().validate_with(&state.config) {
        let errors = e
            .into_inner()
            .iter()
            .map(|(path, error)| {
                json!({
                    "path": path.to_string(),
                    "error": error.to_string()
                })
            })
            .collect::<Vec<_>>();
        return Err((StatusCode::BAD_REQUEST, Json(json!({ "errors": errors }))));
    }

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
#[garde(context(Configs))]
pub struct CreateEntityRequest {
    #[garde(length(min = 1))]
    #[garde(alphanumeric)]
    pub name: String,
    #[garde(length(min = 1))]
    #[garde(custom(valid_client))]
    pub client: String,
    #[garde(length(min = 1))]
    #[garde(alphanumeric)]
    pub source: String,
    #[garde(dive)]
    pub shape: ShapeRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(context(Configs))]
pub struct IdColumnRequest {
    #[garde(custom(validate_path))]
    pub path: String,
    #[serde(rename = "type")]
    #[garde(skip)]
    pub typ: IdType,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(context(Configs))]
pub struct ColumnRequest {
    #[garde(length(min = 1))]
    #[garde(alphanumeric)]
    pub name: String,
    #[serde(rename = "type")]
    #[garde(skip)]
    pub typ: DataType,
    #[garde(custom(validate_path))]
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(context(Configs))]
pub struct IndexRequest {
    #[garde(length(min = 1))]
    #[garde(alphanumeric)]
    pub name: String,
    #[garde(length(min = 1))]
    #[garde(custom(unique_list))]
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[garde(context(Configs))]
pub struct ShapeRequest {
    #[garde(dive)]
    pub id_column: IdColumnRequest,
    #[garde(length(min = 1))]
    #[garde(dive)]
    #[garde(custom(valid_columns))]
    pub columns: Vec<ColumnRequest>,
    #[garde(dive)]
    #[garde(custom(valid_indexes(&self.columns)))]
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

fn validate_path(path: &str, _conf: &Configs) -> garde::Result {
    parse_json_path(path)
        .map(|_| ())
        .map_err(|e| garde::Error::new(format!("Invalid JSONPath: {}, {}", path, e)))
}

fn unique_list(list: &Vec<String>, _conf: &Configs) -> garde::Result {
    let mut seen = HashSet::new();
    for item in list {
        if !seen.insert(item) {
            return Err(garde::Error::new(format!("Duplicate item: {}", item)));
        }
    }
    Ok(())
}

fn valid_client(client: &str, conf: &Configs) -> garde::Result {
    if conf.sources.contains_key(client) {
        Ok(())
    } else {
        Err(garde::Error::new(format!("Invalid client: {}", client)))
    }
}

fn valid_indexes(columns: &Vec<ColumnRequest>) -> impl FnOnce(&Vec<IndexRequest>, &Configs) -> garde::Result {
    move |indexes: &Vec<IndexRequest>, _conf: &Configs| {
        let mut column_names = HashSet::new();
        for column in columns {
            column_names.insert(column.name.clone());
        }
        for index in indexes {
            for column in &index.columns {
                if !column_names.contains(column) {
                    return Err(garde::Error::new(format!("Invalid index column: {}", column)));
                }
            }
        }
        Ok(())
    }
}

fn valid_columns(columns: &Vec<ColumnRequest>, _conf: &Configs) -> garde::Result {
    let mut seen = HashSet::new();
    for column in columns {
        let name = &column.name;
        if !seen.insert(name.clone()) {
            return Err(garde::Error::new(format!("Duplicate column with name: {}", name)));
        }
    }
    if seen.contains("id") {
        return Err(garde::Error::new("custom id column is not allowed"));
    }
    Ok(())
}
