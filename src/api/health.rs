use axum::{extract::State, http::StatusCode, response::Json};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
// Health check endpoints - errors handled with anyhow

use super::AppState;

/// Health check endpoint
pub async fn health_check() -> Json<JsonValue> {
    Json(json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// Detailed health check endpoint
pub async fn detailed_health_check(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let mut health_status = HashMap::new();
    let mut overall_healthy = true;

    // Check PostgreSQL connection
    match sqlx::query("SELECT 1")
        .fetch_optional(state.db_manager.postgres())
        .await
    {
        Ok(_) => {
            health_status.insert(
                "postgresql",
                json!({
                    "status": "healthy",
                    "message": "Connection successful"
                }),
            );
        }
        Err(e) => {
            health_status.insert(
                "postgresql",
                json!({
                    "status": "unhealthy",
                    "message": format!("Connection failed: {}", e)
                }),
            );
            overall_healthy = false;
        }
    }

    // Check entity manager
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

    // Check cache replicator
    match state.cache_replicator.get_stats().await {
        Ok(stats) => {
            health_status.insert(
                "cache_replicator",
                json!({
                    "status": "healthy",
                    "message": "Replication running",
                    "stats": stats
                }),
            );
        }
        Err(e) => {
            health_status.insert(
                "cache_replicator",
                json!({
                    "status": "unhealthy",
                    "message": format!("Failed to get stats: {}", e)
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
