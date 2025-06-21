use axum::{extract::State, http::StatusCode, response::Json, routing::get, Router};
use serde_json::{json, Value as JsonValue};
use sqlx::Row;
use std::time::SystemTime;

use super::AppState;

/// Create system router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/system/stats", get(get_system_stats))
        .route("/system/health", get(get_system_health))
        .route("/system/info", get(get_system_info))
}

/// Get system statistics
async fn get_system_stats(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // Get basic system information
    let uptime = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Get entity count
    let entities_count = match state.entity_manager.get_all_entities().await {
        Ok(entities) => entities.len(),
        Err(_) => 0,
    };

    // Get oplog counts
    let main_count = sqlx::query("SELECT COUNT(*) as count FROM oplog")
        .fetch_optional(state.db_manager.postgres())
        .await
        .ok()
        .flatten()
        .map(|row| row.get::<i64, _>("count"))
        .unwrap_or(0);

    let staging_count = sqlx::query("SELECT COUNT(*) as count FROM oplog_staging")
        .fetch_optional(state.db_manager.postgres())
        .await
        .ok()
        .flatten()
        .map(|row| row.get::<i64, _>("count"))
        .unwrap_or(0);

    Ok(Json(json!({
        "uptime_seconds": uptime,
        "timestamp": chrono::Utc::now(),
        "entities_count": entities_count,
        "oplogs": {
            "main": main_count,
            "staging": staging_count,
            "total": main_count + staging_count
        },
        "system": {
            "platform": std::env::consts::OS,
            "architecture": std::env::consts::ARCH,
            "process_id": std::process::id()
        }
    })))
}

/// Get comprehensive system health
async fn get_system_health(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let mut overall_healthy = true;
    let mut components = json!({});

    // Check PostgreSQL
    match sqlx::query("SELECT 1")
        .fetch_optional(state.db_manager.postgres())
        .await
    {
        Ok(_) => {
            components["postgresql"] = json!({
                "status": "healthy",
                "message": "Connection successful"
            });
        }
        Err(e) => {
            components["postgresql"] = json!({
                "status": "unhealthy",
                "error": e.to_string()
            });
            overall_healthy = false;
        }
    }

    // Check entity manager
    match state.entity_manager.get_all_entities().await {
        Ok(entities) => {
            components["entity_manager"] = json!({
                "status": "healthy",
                "entities_count": entities.len()
            });
        }
        Err(e) => {
            components["entity_manager"] = json!({
                "status": "unhealthy",
                "error": e.to_string()
            });
            overall_healthy = false;
        }
    }

    // Check cache replicator
    match state.cache_replicator.get_stats().await {
        Ok(stats) => {
            components["cache_replicator"] = json!({
                "status": "healthy",
                "last_processed_id": stats.last_processed_id,
                "pending_oplogs": stats.pending_oplogs
            });
        }
        Err(e) => {
            components["cache_replicator"] = json!({
                "status": "degraded",
                "error": e.to_string()
            });
        }
    }

    let response = json!({
        "overall_status": if overall_healthy { "healthy" } else { "unhealthy" },
        "timestamp": chrono::Utc::now(),
        "components": components
    });

    if overall_healthy {
        Ok(Json(response))
    } else {
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(response)))
    }
}

/// Get system information
async fn get_system_info(
    State(_state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    Ok(Json(json!({
        "application": {
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "rust_version": "1.75+",
            "build_timestamp": chrono::Utc::now().to_rfc3339()
        },
        "system": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "process_id": std::process::id(),
            "hostname": std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string())
        },
        "timestamp": chrono::Utc::now()
    })))
}
