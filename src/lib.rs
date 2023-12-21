use const_format::formatcp;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::Inet;
use predicates::prelude::*;
use predicates::BoxPredicate;
use simple_logger::SimpleLogger;
use std::collections::HashMap;
use std::net::IpAddr;
use std::process;
use std::thread::JoinHandle;
use std::time::Duration;
use std::{thread, time};
use tokio::runtime::Builder;

mod config;
mod handlers;
mod server;

use config::ServerConfig;

use crate::server::run_server;

const WORKER_NAME: &str = "http server";
const SCHEMA_NAME: &str = "http_server";

const QUERY_SERVERS: &str = formatcp!(
    "
SELECT
  listen_port,
  '::1'::inet as server_host,
  current_setting('port')::integer as server_port,
  postgres_user,
  postgres_pass,
  database_name
FROM
  {SCHEMA_NAME}.servers
"
);

const ADMINISTRATION_SCHEMA: &str = formatcp!(
    "
CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.servers (
    listen_port integer NOT NULL PRIMARY KEY,
    postgres_user varchar NOT NULL DEFAULT 'postgres',
    postgres_pass varchar NOT NULL DEFAULT 'postgres',
    database_name varchar NOT NULL DEFAULT 'postgres');
CREATE OR REPLACE FUNCTION {SCHEMA_NAME}.on_servers_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_reload_conf();
  RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS on_servers_changed ON {SCHEMA_NAME}.servers;
CREATE TRIGGER on_servers_changed AFTER INSERT OR UPDATE OR DELETE ON {SCHEMA_NAME}.servers FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA_NAME}.on_servers_changed();
"
);

pgrx::pg_module_magic!();

#[derive(Debug)]
struct ServerState {
    join_handle: Option<thread::JoinHandle<()>>,
    signal_sender: tokio::sync::oneshot::Sender<()>,
    server_config: config::ServerConfig,
}

#[pg_guard]
pub extern "C" fn _PG_init() {
    BackgroundWorkerBuilder::new(WORKER_NAME)
        .set_function("pg_graphql_server_main")
        .set_library("pg_graphql_server")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .enable_spi_access()
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn pg_graphql_server_main(_arg: pg_sys::Datum) {
    let name = BackgroundWorker::get_name();

    // Initialize the log
    SimpleLogger::new().init().unwrap();

    // These are the signals we want to receive.  If we don't attach the SIGTERM handler, then
    // we'll never be able to exit via an external notification
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // We want to be able to use SPI against the specified database (postgres), as the superuser which
    // did the initdb.
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);
    //BackgroundWorker::connect_worker_to_spi(None, None);

    // Initialize the administration schema
    BackgroundWorker::transaction(|| {
        Spi::connect(
            |mut client| match client.update(ADMINISTRATION_SCHEMA, None, None) {
                Ok(_) => log!("initialized administration schema"),
                Err(_) => warning!("failed to initialize administration schema"),
            },
        );
    });

    log!("background worker {name} started");

    // The list of server threads we are managing
    let mut servers = HashMap::<i32, ServerState>::new();

    // Wake up every 60s or if we received a signal
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(60))) {
        // Reload configuration
        if BackgroundWorker::sighup_received() {
            // Short delay because normally we are triggered from within a
            // transaction and we need the transaction to be committed before
            // we see the changes.
            thread::sleep(time::Duration::from_millis(500));
        }

        // Clean up servers that has exited prematurely
        for port in servers.keys().cloned().collect::<Vec<i32>>() {
            let server = servers.get(&port).unwrap();
            let is_finished = match server.join_handle.as_ref() {
                Some(join_handle) => join_handle.is_finished(),
                None => true,
            };
            if is_finished {
                warning!("cleaning up failed server on port {port}");
                servers.remove(&port);
            }
        }

        // Query our administration table and compute the differences with the
        // servers we currently have running.
        if let Some(wanted) = BackgroundWorker::transaction(|| {
            Spi::connect(|client| match client.select(QUERY_SERVERS, None, None) {
                Ok(result) => Some(result.fold(
                    HashMap::<i32, ServerConfig>::new(),
                    |mut acc, row| {
                        let port = row
                            .get_datum_by_ordinal(1)
                            .unwrap()
                            .value::<i32>()
                            .expect("Expected an integer")
                            .expect("Expected port to not be null");
                        let server_config = ServerConfig {
                            listen_port: port as u16,
                            server_host: row
                                .get::<Inet>(2)
                                .unwrap()
                                .unwrap()
                                .0
                                .as_str()
                                .parse::<IpAddr>()
                                .unwrap(),
                            server_port: row.get::<i32>(3).unwrap().unwrap().clamp(0, 65535) as u16,
                            postgres_user: row.get(4).unwrap().unwrap(),
                            postgres_pass: row.get(5).unwrap().unwrap(),
                            database_name: row.get(6).unwrap().unwrap(),
                        };
                        acc.insert(port, server_config);
                        acc
                    },
                )),
                Err(_) => None,
            })
        }) {
            // Determine the servers (by port) that need to be create, updated
            // and removed (basically difference and intersection operations).
            let create = wanted
                .iter()
                .filter_map(|entry| match servers.contains_key(entry.0) {
                    true => None,
                    false => Some(*entry.0),
                });
            let update = servers.iter().filter_map(|entry| {
                match wanted
                    .get(entry.0)
                    .map_or_else(|| false, |config| *config != entry.1.server_config)
                {
                    true => Some(*entry.0),
                    false => None,
                }
            });
            let remove = servers
                .iter()
                .filter_map(|entry| match wanted.contains_key(entry.0) {
                    true => None,
                    false => Some(*entry.0),
                });

            let create_count = create.clone().count();
            let update_count = update.clone().count();
            let remove_count = remove.clone().count();
            if create_count + update_count + remove_count > 0 {
                log!("updating HTTP servers: {create_count} to create, {update_count} to update and {remove_count} to remove");

                let ports_to_create = create.chain(update.clone()).collect::<Vec<i32>>();
                let port_should_be_created = predicate::in_iter(ports_to_create);
                let ports_to_destroy = update.chain(remove).collect::<Vec<i32>>();
                let port_should_be_destroyed = predicate::in_iter(ports_to_destroy);

                destroy_servers(&mut servers, &BoxPredicate::new(port_should_be_destroyed));
                create_servers(
                    &mut servers,
                    &wanted,
                    &BoxPredicate::new(port_should_be_created),
                );
            }
        }
    }

    // Gracefully shutdown servers
    destroy_servers(&mut servers, &BoxPredicate::new(predicate::always()));

    log!("background worker {name} shutdown");
}

fn create_servers(
    servers: &mut HashMap<i32, ServerState>,
    configs: &HashMap<i32, ServerConfig>,
    predicate: &BoxPredicate<i32>,
) {
    let worker_pid: libc::pid_t = process::id().try_into().unwrap();

    // Create all servers that need to be created
    for (port, c) in configs.iter().filter(|entry| predicate.eval(entry.0)) {
        let config = c.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        match thread::Builder::new().spawn(move || {
            Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async {
                    match run_server(config.clone(), rx).await {
                        Ok(_) => log::info!("Server done"),
                        Err(e) => {
                            log::warn!("Server thread exited with error: {}", e.to_string());
                            thread::sleep(Duration::from_millis(2500));
                            unsafe {
                                // Signal our background worker so the server
                                // is restarted.
                                libc::kill(worker_pid, libc::SIGHUP);
                            }
                        }
                    }
                });
        }) {
            Ok(join_handle) => {
                servers.insert(
                    *port,
                    ServerState {
                        join_handle: Some(join_handle),
                        signal_sender: tx,
                        server_config: c.clone(),
                    },
                );
                log!("created server on port {port}")
            }
            Err(_) => warning!("failed to create server on port {port}"),
        }
    }
}

fn destroy_servers(servers: &mut HashMap<i32, ServerState>, predicate: &BoxPredicate<i32>) {
    // A couple of steps are taken here. First we remove the servers we need to
    // destroy from the administration. Then we signal them and finally we wait
    // on each thread. The waiting is done sequentially which is fine from a
    // duration perspective because the shutdown is done in parallel because we
    // signaled all thread first.
    let mut servers_to_join: Vec<(i32, Option<JoinHandle<()>>)> = vec![];

    // We need a copy of the keys in order to please the borrow checker.
    let ports_to_destroy: Vec<i32> = servers
        .keys()
        .cloned()
        .filter(|x| predicate.eval(x))
        .collect();

    // Signal servers that need to be removed.
    for port in ports_to_destroy {
        let mut server = servers.remove(&port).unwrap();
        match server.signal_sender.send(()) {
            Ok(_) => (),
            Err(_) => warning!("failed to send shutdown signal to server on port {port}"),
        };
        servers_to_join.push((port, server.join_handle.take()));
    }

    // Join on all servers we signaled to stop.
    for (port, maybe_join_handle) in servers_to_join.iter_mut() {
        if let Some(join_handle) = maybe_join_handle.take() {
            match join_handle.join() {
                Ok(_) => log!("removed server on port {port}"),
                Err(_) => warning!("failed to wait for server on port {port}"),
            }
        }
    }
}

// async fn thread_main_test(
//     config: ServerConfig,
//     rx: tokio::sync::oneshot::Receiver<()>,
// ) -> anyhow::Result<()> {
//     println!("Hello from inside thread on port {:?}", config);
//     match SystemTime::now().duration_since(UNIX_EPOCH) {
//         Ok(n) => {
//             if n.as_secs() % 2 == 0 {
//                 match rx.await {
//                     Ok(_) => println!("Shutdown signal received"),
//                     Err(e) => {
//                         println!("Failed to read oneshot signal: {:?}", e)
//                     }
//                 };
//             } else {
//                 tokio::time::sleep(Duration::from_millis(5000)).await;
//                 println!("#### Simulated error exit from thread");
//                 return Err(anyhow!("Timeout error simulation"));
//             }
//         }
//         Err(_) => panic!("SystemTime before UNIX EPOCH!"),
//     }
//     println!("Bye from inside thread");

//     Ok(())
// }
