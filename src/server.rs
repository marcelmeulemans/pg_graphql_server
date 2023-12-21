use anyhow::bail;
use axum::{extract::State, routing::get, Router};
use pgrx::prelude::*;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Row,
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};
use thiserror::Error;

use crate::{asset_handler, config::ServerConfig};

#[derive(Error, Debug)]
enum ServerErrors {
    #[error("the pg_graphql extension is not available")]
    GraphQlExtensionNotAvailable,
}

use crate::handlers;

pub async fn run_server(
    config: ServerConfig,
    rx: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let socket = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), config.listen_port);

    log::debug!("initializing http server on {}", socket);

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
            .await?,
    };

    log::info!(
        "connected to database {} as user {}",
        config.database_name,
        config.postgres_user
    );

    // TODO: Remove this? Do we really need to check, or should the graphql handler just return an error if not the pg_graphql extension is not installed. Or maybe run this if the using the extension fails.
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
        .route("/", get(asset_handler!("index.html")))
        .route("/health", get(handlers::health_handler))
        .route(
            "/graphql",
            get(handlers::graphiql_handler).post(handlers::graphql_handler),
        )
        .route("/*path", get(handlers::asset_path_handler))
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
