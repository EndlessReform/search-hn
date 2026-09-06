//! Small consistent error envelope; never reinterpret backend failure as a raw-vector success.
use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::json;

pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
    pub fn bad_input(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }
    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, "backend_error", message)
    }
    pub fn transport(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::new(
                StatusCode::GATEWAY_TIMEOUT,
                "backend_timeout",
                "vLLM request deadline exceeded",
            )
        } else {
            tracing::warn!(%error, "vLLM transport failed");
            Self::backend("cannot complete request to vLLM")
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(json!({"error": {
                "message": self.message, "type": self.code, "code": self.code
            }})),
        )
            .into_response();
        if self.status == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "1".parse().unwrap());
        }
        response
    }
}
