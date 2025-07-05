use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tracing::error;

use super::AppState;
use crate::types::Oplog;

/// Create oplog router
pub fn router() -> Router<AppState> {
    Router::new().route("/oplogs", get(get_oplogs))
}

/// Get oplogs with pagination
async fn get_oplogs(
    State(state): State<AppState>,
    Query(params): Query<OplogRequest>,
) -> Result<Json<Vec<Oplog>>, (StatusCode, Json<JsonValue>)> {
    let from = params.from.clamp(0, i64::MAX);
    let limit = params.limit.clamp(1, 1000);

    match state.oplog_db.get_oplogs(&params.entities, from, limit).await {
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

#[derive(Deserialize, Debug)]
pub struct OplogRequest {
    pub entities: Vec<String>,
    pub from: i64,
    pub limit: i64,
}
