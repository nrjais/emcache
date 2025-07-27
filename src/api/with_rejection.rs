use axum::{
    Json,
    extract::rejection::JsonRejection,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use serde_qs::axum::QsQueryRejection;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    JsonExtractorRejection(#[from] JsonRejection),
    #[error(transparent)]
    QsQueryExtractorRejection(#[from] QsQueryRejection),
}

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
