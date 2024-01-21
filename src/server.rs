use crate::handlers;
use crate::{asset_handler, config::ServerConfig};
use axum::{extract::State, routing::get, Router};
use pgrx::prelude::*;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions,
};
use std::{net::Ipv4Addr, time::Duration};
use thiserror::Error;
use tokio::net::TcpListener;

#[derive(Error, Debug)]
pub enum ServerFailedError {
    #[error("database connetion failed")]
    DatabaseConnection,
    #[error("failed to start http server")]
    Bind,
    #[error(transparent)]
    Serve(#[from] std::io::Error),
}

pub async fn run_server(
    config: ServerConfig,
    rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), ServerFailedError> {
    let socket = (Ipv4Addr::UNSPECIFIED, config.listen_port);

    tracing::debug!("initializing http server on {}:{}", socket.0, socket.1);

    let state = handlers::ServerContext {
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
            .await
            .map_err(|_| ServerFailedError::DatabaseConnection)?,
    };

    tracing::info!(
        "connected to database {} as user {}",
        config.database_name,
        config.postgres_user
    );

    let router = Router::new()
        .route("/", get(asset_handler!("index.html")))
        .route("/health", get(handlers::health_handler))
        .route(
            "/graphql",
            get(handlers::graphiql_handler).post(handlers::graphql_handler),
        )
        .route("/*path", get(handlers::asset_path_handler))
        .with_state(state)
        .layer((
            tower_http::trace::TraceLayer::new_for_http(),
            tower_http::timeout::TimeoutLayer::new(Duration::from_secs(10)),
        ));

    tracing::info!("http server start listen on {}:{}", socket.0, socket.1);

    let listener = TcpListener::bind(socket)
        .await
        .map_err(|_| ServerFailedError::Bind)?;

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            rx.await.ok();
        })
        .await
        .map_err(ServerFailedError::Serve)?;

    tracing::debug!("http server on {}:{} has shutdown", socket.0, socket.1);

    Ok(())
}
