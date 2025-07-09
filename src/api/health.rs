use axum::response::Json;
use serde_json::{Value as JsonValue, json};

pub async fn health_check() -> Json<JsonValue> {
    Json(json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}
