#[derive(Clone)]
pub struct ServerContext {
    pub pool: sqlx::Pool<sqlx::Postgres>,
}
