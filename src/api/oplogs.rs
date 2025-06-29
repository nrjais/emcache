use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use super::AppState;

/// Create oplog router
pub fn router() -> Router<AppState> {
    Router::new().route("/oplogs", get(get_oplogs))
}

/// Get oplogs with pagination
async fn get_oplogs(
    State(state): State<AppState>,
    Query(params): Query<OplogRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    todo!();
}

#[derive(Deserialize)]
pub struct OplogRequest {
    pub entities: Vec<String>,
    pub offset: i64,
    pub limit: i64,
}
