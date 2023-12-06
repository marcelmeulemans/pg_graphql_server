use anyhow::bail;
use async_graphql_axum::GraphQLRequest;
use axum::{
    extract::State,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use pgrx::prelude::*;
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Pool, Row,
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};
use thiserror::Error;

use crate::config::ServerConfig;

#[derive(Error, Debug)]
enum ServerErrors {
    #[error("the pg_graphql extension is not available")]
    GraphQlExtensionNotAvailable,
}

#[derive(Clone)]
struct ServerContext {
    pool: Pool<sqlx::Postgres>,
}

async fn health_handler(State(state): State<ServerContext>) -> Json<Value> {
    Json(json!({
        "pool": {
            "size": state.pool.size(),
            "idle": state.pool.num_idle() as u32
        }
    }))
}

async fn graphql_handler(
    State(state): State<ServerContext>,
    req: GraphQLRequest,
) -> Result<Json<Value>, HandlerError> {
    let json: Value = sqlx::query_scalar("SELECT graphql.resolve($1)")
        .bind(req.0.query)
        .fetch_one(&state.pool)
        .await?;
    Ok(Json(json))
}

struct HandlerError(sqlx::Error);

// impl<E> From<E> for AppError
// where
//     E: Into<sqlx::Error>,
// {
//     fn from(err: E) -> Self {
//         Self(err.into())
//     }
// }

impl From<sqlx::Error> for HandlerError {
    fn from(e: sqlx::Error) -> Self {
        Self(e.into())
    }
}

impl IntoResponse for HandlerError {
    #[cfg(debug_assertions)]
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Internal Server Error: {}", self.0),
        )
            .into_response()
    }

    #[cfg(not(debug_assertions))]
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
    }
}

pub async fn run_server(
    config: ServerConfig,
    rx: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let socket = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), config.listen_port);

    log::debug!("initializing http server on {}", socket);

    let state = ServerContext {
        pool: PgPoolOptions::new()
            .max_connections(5)
            .min_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .max_lifetime(Duration::from_secs(3600))
            .connect_with(
                PgConnectOptions::new()
                    .host(&config.server_host.to_string())
                    .port(config.server_port)
                    .username(&config.postgres_user)
                    .password(&config.postgres_pass)
                    .database(&config.database_name)
                    .log_statements(log::LevelFilter::Trace),
            )
            .await?,
    };

    log::info!(
        "connected to database {} as user {}",
        config.database_name,
        config.postgres_user
    );

    match sqlx::query("SELECT default_version AS available_version, (SELECT extversion FROM pg_catalog.pg_extension WHERE extname=name) AS installed_version FROM pg_available_extensions() WHERE name='pg_graphql'")
        .fetch_optional(&state.pool)
        .await? {
            Some(row) => {
                if let Ok(available_version) = row.try_get::<String, usize>(0) {
                    if let Ok(installed_version) = row.try_get::<String, usize>(1) {
                        log::debug!("detected pg_graphql version {}", installed_version);
                    } else {
                        log::warn!("extension pg_graphql {} is available but not installed", available_version);
                    }
                } else {
                    bail!(ServerErrors::GraphQlExtensionNotAvailable)
                }
            },
            None => bail!(ServerErrors::GraphQlExtensionNotAvailable)
        };

    let router = Router::new()
        .route("/health", get(health_handler))
        .route("/graphql", get(graphql_handler).post(graphql_handler))
        .with_state(state);

    log::info!("http server start listen on {}", socket);

    axum::Server::bind(&socket)
        .serve(router.into_make_service())
        .with_graceful_shutdown(async {
            rx.await.ok();
        })
        .await?;

    log::debug!("http server on {} has shutdown", socket);

    Ok(())
}
