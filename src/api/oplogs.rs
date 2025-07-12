use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
};
use axum_extra::extract::WithRejection;
use garde::Validate;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tracing::error;

use super::AppState;
use crate::{api::with_rejection::ApiError, types::Oplog};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/oplogs", get(get_oplogs))
        .route("/oplogs/status", get(get_oplog_status))
}

async fn get_oplogs(
    State(state): State<AppState>,
    WithRejection(Query(params), _): WithRejection<Query<OplogRequest>, ApiError>,
) -> Result<Json<Vec<Oplog>>, (StatusCode, Json<JsonValue>)> {
    if let Err(e) = params.validate() {
        let errors = e
            .into_inner()
            .iter()
            .map(|(path, error)| format!("{}: {}", path.to_string(), error.to_string()))
            .collect::<Vec<_>>();
        return Err((StatusCode::BAD_REQUEST, Json(json!({ "errors": errors }))));
    }

    let from = params.from.clamp(0, i64::MAX);
    let limit = params.limit.clamp(1, 10000);

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

#[derive(Deserialize, Debug, Validate)]
pub struct OplogRequest {
    #[garde(range(min = 0))]
    pub from: i64,
    #[garde(range(min = 1, max = 10000))]
    pub limit: i64,
    #[serde(default)]
    #[garde(length(min = 1))]
    pub entities: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct OplogStatus {
    pub max_id: i64,
}
