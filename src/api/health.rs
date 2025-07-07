use super::AppState;
use axum::{extract::State, http::StatusCode, response::Json};
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;

pub async fn health_check() -> Json<JsonValue> {
    Json(json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

pub async fn detailed_health_check(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let mut health_status = HashMap::new();
    let mut overall_healthy = true;

    match state.entity_manager.get_all_entities().await {
        Ok(entities) => {
            health_status.insert(
                "entity_manager",
                json!({
                    "status": "healthy",
                    "message": format!("Managing {} entities", entities.len())
                }),
            );
        }
        Err(e) => {
            health_status.insert(
                "entity_manager",
                json!({
                    "status": "unhealthy",
                    "message": format!("Failed to get entities: {}", e)
                }),
            );
            overall_healthy = false;
        }
    }

    let response = json!({
        "status": if overall_healthy { "healthy" } else { "unhealthy" },
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "components": health_status
    });

    if overall_healthy {
        Ok(Json(response))
    } else {
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(response)))
    }
}
