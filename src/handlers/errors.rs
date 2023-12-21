use axum::response::IntoResponse;
use axum::{http::StatusCode, Json};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HandlerError {
    #[error("graphql error")]
    GraphqlError(async_graphql::ServerError),
    #[error(transparent)]
    DatabaseError(#[from] sqlx::Error),
}

impl IntoResponse for HandlerError {
    fn into_response(self) -> axum::response::Response {
        match self {
            HandlerError::GraphqlError(e) => (StatusCode::BAD_REQUEST, Json(e)).into_response(),
            HandlerError::DatabaseError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"message": e.to_string()})),
            )
                .into_response(),
        }
    }
}
