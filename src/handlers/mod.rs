mod context;
pub use context::ServerContext;

mod errors;

mod graphiql;
pub use graphiql::graphiql_handler;

mod health;
pub use health::health_handler;

mod graphql;
pub use graphql::graphql_handler;

mod asset;
pub use asset::{asset_path_handler, get_asset_response};

#[macro_export]
macro_rules! asset_handler {
    ($path:expr) => {{
        |State(state): State<handlers::ServerContext>| async move {
            let span = tracing::info_span!("asset_handler", path = $path);
            let _enter = span.enter();
            handlers::get_asset_response(&state.pool, $path).await
        }
    }};
}
