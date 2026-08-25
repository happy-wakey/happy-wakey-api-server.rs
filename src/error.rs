use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use happy_wakey_interfaces::ApiError;

#[derive(Debug, thiserror::Error)]
pub enum ApiFailure {
    #[error("unauthorized")]
    Unauthorized,
    #[error("Shared Auth unavailable")]
    AuthUnavailable,
    #[error("not found")]
    NotFound,
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("database unavailable")]
    Database(#[from] sea_orm::DbErr),
    #[error("contract violation: {0}")]
    Contract(String),
}

impl ApiFailure {
    pub fn unauthorized() -> Self {
        Self::Unauthorized
    }

    pub fn auth_unavailable() -> Self {
        Self::AuthUnavailable
    }
}

impl IntoResponse for ApiFailure {
    fn into_response(self) -> Response {
        let (status, code, retryable) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized", false),
            Self::AuthUnavailable => (StatusCode::SERVICE_UNAVAILABLE, "auth_unavailable", true),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found", false),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict", false),
            Self::Invalid(_) => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_request", false),
            Self::Database(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "database_unavailable",
                true,
            ),
            Self::Contract(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "contract_violation",
                false,
            ),
        };
        let message = match &self {
            Self::Database(_) => "database unavailable".to_owned(),
            _ => self.to_string(),
        };
        (
            status,
            Json(ApiError {
                code: code.into(),
                message,
                retryable,
                trace_id: None,
            }),
        )
            .into_response()
    }
}
