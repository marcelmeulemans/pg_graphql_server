use axum::{extract::State, Json};
use serde_json::{json, Value};

use super::context::ServerContext;

pub async fn health_handler(State(state): State<ServerContext>) -> Json<Value> {
    Json(json!({
        "pool": {
            "size": state.pool.size(),
            "idle": state.pool.num_idle() as u32
        }
    }))
}
