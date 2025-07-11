use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tracing::error;

use super::AppState;
use crate::types::Oplog;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/oplogs", get(get_oplogs))
        .route("/oplogs/status", get(get_oplog_status))
}

async fn get_oplogs(
    State(state): State<AppState>,
    Query(params): Query<OplogRequest>,
) -> Result<Json<Vec<Oplog>>, (StatusCode, Json<JsonValue>)> {
    let from = params.from.clamp(0, i64::MAX);
    let limit = params.limit.clamp(1, 1000);

    match state.oplog_db.get_oplogs_by_entity(&params.entities, from, limit).await {
        Ok(oplogs) => Ok(Json(oplogs)),
        Err(e) => {
            error!("Failed to get oplogs: {:?}, request: {:?}", e, params);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get oplogs", "message": e.to_string() })),
            ))
        }
    }
}

async fn get_oplog_status(State(state): State<AppState>) -> Result<Json<OplogStatus>, (StatusCode, Json<JsonValue>)> {
    match state.oplog_db.get_max_oplog_id().await {
        Ok(max_id) => Ok(Json(OplogStatus {
            max_id: max_id.unwrap_or(0),
        })),
        Err(e) => {
            error!("Failed to get oplog status: {:?}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get oplog status", "message": e.to_string() })),
            ))
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct OplogRequest {
    pub entities: Vec<String>,
    pub from: i64,
    pub limit: i64,
}

#[derive(Serialize, Debug)]
pub struct OplogStatus {
    pub max_id: i64,
}
