use async_graphql::http::GraphiQLSource;
use axum::response::{Html, IntoResponse};
use tracing::instrument;

#[instrument]
pub async fn graphiql_handler() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}
