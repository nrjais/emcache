use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tracing::error;

use super::AppState;
use crate::cache_replicator::ReplicationStats;

/// Create replication router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/replication/stats", get(get_replication_stats))
        .route("/replication/status", get(get_replication_status))
        .route("/replication/health", get(get_replication_health))
        .route("/replication/reset", post(reset_replication))
        .route("/replication/force-sync", post(force_sync))
}

/// Get replication statistics
async fn get_replication_stats(
    State(state): State<AppState>,
) -> Result<Json<ReplicationStats>, (StatusCode, Json<JsonValue>)> {
    match state.cache_replicator.get_stats().await {
        Ok(stats) => Ok(Json(stats)),
        Err(e) => {
            error!("Failed to get replication stats: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get replication stats" })),
            ))
        }
    }
}

/// Get replication status
async fn get_replication_status(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    match state.cache_replicator.get_stats().await {
        Ok(stats) => {
            let is_healthy = stats.failed_operations < 10; // Arbitrary threshold
            let replication_lag = stats.total_oplogs - stats.processed_oplogs;

            Ok(Json(json!({
                "status": if is_healthy { "running" } else { "degraded" },
                "is_active": true,
                "last_processed_id": stats.last_processed_id,
                "replication_lag": replication_lag,
                "processing_rate": "real-time",
                "health_status": if is_healthy { "healthy" } else { "degraded" },
                "timestamp": chrono::Utc::now()
            })))
        }
        Err(e) => {
            error!("Failed to get replication status: {}", e);
            Ok(Json(json!({
                "status": "error",
                "is_active": false,
                "error": e.to_string(),
                "timestamp": chrono::Utc::now()
            })))
        }
    }
}

/// Get detailed replication health
async fn get_replication_health(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    match state.cache_replicator.get_stats().await {
        Ok(stats) => {
            let replication_lag = stats.total_oplogs - stats.processed_oplogs;
            let error_rate = if stats.processed_oplogs > 0 {
                (stats.failed_operations as f64 / stats.processed_oplogs as f64) * 100.0
            } else {
                0.0
            };

            let health_status = if stats.failed_operations > 50 {
                "critical"
            } else if stats.failed_operations > 10 || replication_lag > 1000 {
                "warning"
            } else {
                "healthy"
            };

            Ok(Json(json!({
                "overall_health": health_status,
                "metrics": {
                    "replication_lag": replication_lag,
                    "error_rate_percent": error_rate,
                    "total_processed": stats.processed_oplogs,
                    "total_failed": stats.failed_operations,
                    "pending_oplogs": stats.pending_oplogs
                },
                "alerts": {
                    "high_error_rate": error_rate > 5.0,
                    "high_replication_lag": replication_lag > 1000,
                    "processing_stalled": stats.pending_oplogs > stats.total_oplogs * 2
                },
                "timestamp": chrono::Utc::now()
            })))
        }
        Err(e) => {
            error!("Failed to get replication health: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get replication health" })),
            ))
        }
    }
}

/// Reset replication (clear state and restart)
async fn reset_replication(
    State(_state): State<AppState>,
    Json(_request): Json<ResetRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // In a real implementation, this would:
    // 1. Stop current replication
    // 2. Clear replication state
    // 3. Restart replication from specified point

    // For now, return a placeholder response
    Ok(Json(json!({
        "message": "Replication reset initiated",
        "status": "pending",
        "timestamp": chrono::Utc::now(),
        "note": "This is a placeholder - actual reset logic needs implementation"
    })))
}

/// Force synchronization of all entities
async fn force_sync(
    State(_state): State<AppState>,
    Json(request): Json<ForceSyncRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // In a real implementation, this would:
    // 1. Queue immediate sync for specified entities
    // 2. Return job/task ID for tracking

    let entities_to_sync = if request.entities.is_empty() {
        vec!["all".to_string()]
    } else {
        request.entities
    };

    Ok(Json(json!({
        "message": "Force sync initiated",
        "entities": entities_to_sync,
        "sync_type": request.sync_type.unwrap_or_else(|| "incremental".to_string()),
        "task_id": format!("sync_{}", chrono::Utc::now().timestamp()),
        "timestamp": chrono::Utc::now(),
        "note": "This is a placeholder - actual sync logic needs implementation"
    })))
}

#[derive(Deserialize)]
pub struct ResetRequest {
    pub from_oplog_id: Option<i64>,
    pub clear_cache: Option<bool>,
}

#[derive(Deserialize)]
pub struct ForceSyncRequest {
    pub entities: Vec<String>,
    pub sync_type: Option<String>, // "full" or "incremental"
}
