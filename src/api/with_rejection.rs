use axum::{
    Json,
    extract::rejection::JsonRejection,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use serde_qs::axum::QsQueryRejection;
use thiserror::Error;

// We derive `thiserror::Error`
#[derive(Debug, Error)]
pub enum ApiError {
    // The `#[from]` attribute generates `From<JsonRejection> for ApiError`
    // implementation. See `thiserror` docs for more information
    #[error(transparent)]
    JsonExtractorRejection(#[from] JsonRejection),
    #[error(transparent)]
    QsQueryExtractorRejection(#[from] QsQueryRejection),
}

// We implement `IntoResponse` so ApiError can be used as a response
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::JsonExtractorRejection(json_rejection) => (json_rejection.status(), json_rejection.body_text()),
            ApiError::QsQueryExtractorRejection(query_rejection) => {
                (StatusCode::BAD_REQUEST, query_rejection.to_string())
            }
        };

        let payload = json!({
            "reason": message,
        });

        (status, Json(payload)).into_response()
    }
}
