use axum::{extract::State, Json};
use serde_json::{json, Value};
use tracing::instrument;

use super::context::ServerContext;

#[instrument(skip(state))]
pub async fn health_handler(State(state): State<ServerContext>) -> Json<Value> {
    Json(json!({
        "pool": {
            "size": state.pool.size(),
            "idle": state.pool.num_idle() as u32
        }
    }))
}
