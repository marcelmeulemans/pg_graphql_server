use std::net::IpAddr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    pub listen_port: u16,
    pub server_host: IpAddr,
    pub server_port: u16,
    pub postgres_user: String,
    pub postgres_pass: String,
    pub database_name: String,
}
