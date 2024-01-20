use super::{context::ServerContext, errors::HandlerError};
use async_graphql_axum::GraphQLRequest;
use axum::{extract::State, Json};
use serde_json::Value;
use tracing::{instrument, Instrument};

#[instrument(skip(state, req), fields(operation = req.0.operation_name, query = req.0.query))]
pub async fn graphql_handler(
    State(state): State<ServerContext>,
    mut req: GraphQLRequest,
) -> Result<Json<Value>, HandlerError> {
    // Parse the query here to make sure it's valid graphql. We don't want to
    // waste a connection just to find out the query is not valid (even if it
    // means double parsing).
    let _ = req.0.parsed_query().map_err(HandlerError::GraphqlError)?;
    let json: Value = sqlx::query_scalar("SELECT graphql.resolve($1)")
        .bind(req.0.query)
        .fetch_one(&state.pool)
        .instrument(tracing::debug_span!("query_resolver"))
        .await
        .map_err(|e| {
            tracing::warn!("failed to resolve graphql query: {e}");
            HandlerError::GraphqlExtensionNotAvailable
        })?;
    Ok(Json(json))
}
