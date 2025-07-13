use crate::api::AppState;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use serde_json::{Value as JsonValue, json};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

pub fn router() -> Router<AppState> {
    Router::new().route("/snapshot/{name}", get(get_snapshot))
}

async fn get_snapshot(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<JsonValue>)> {
    let snapshot = state.snapshot_manager.snapshot(&name).await.map_err(|e| {
        let res = json!({ "error": format!("Failed to get snapshot: {}", e) });
        (StatusCode::NOT_FOUND, Json(res))
    })?;

    let stream = ReaderStream::new(File::from_std(snapshot));
    let body = Body::from_stream(stream);
    let headers = [
        (header::CONTENT_TYPE, "application/octet-stream"),
        (header::CONTENT_DISPOSITION, &format!("attachment; filename={name}.db")),
    ];

    Ok((headers, body).into_response())
}
