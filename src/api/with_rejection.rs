use axum::{
    Json,
    extract::rejection::{JsonRejection, QueryRejection},
    response::IntoResponse,
};
use serde_json::json;
use thiserror::Error;

// We derive `thiserror::Error`
#[derive(Debug, Error)]
pub enum ApiError {
    // The `#[from]` attribute generates `From<JsonRejection> for ApiError`
    // implementation. See `thiserror` docs for more information
    #[error(transparent)]
    JsonExtractorRejection(#[from] JsonRejection),
    #[error(transparent)]
    QueryExtractorRejection(#[from] QueryRejection),
}

// We implement `IntoResponse` so ApiError can be used as a response
impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::JsonExtractorRejection(json_rejection) => (json_rejection.status(), json_rejection.body_text()),
            ApiError::QueryExtractorRejection(query_rejection) => {
                (query_rejection.status(), query_rejection.body_text())
            }
        };

        let payload = json!({
            "reason": message,
        });

        (status, Json(payload)).into_response()
    }
}
