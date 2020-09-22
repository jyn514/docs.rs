use crate::metrics::Metrics;
use crate::{db::Client, Blocking, Config};
use sqlx::Executor;
use std::sync::Arc;

pub type PoolClient = sqlx::pool::PoolConnection<sqlx::Postgres>;

const DEFAULT_SCHEMA: &str = "public";

#[derive(Debug, Clone)]
pub struct Pool {
    pool: sqlx::Pool<sqlx::Postgres>,
    metrics: Arc<Metrics>,
    max_size: u32,
}

impl Pool {
    pub fn new(config: &Config, metrics: Arc<Metrics>) -> Result<Pool, PoolError> {
        Self::new_inner(config, metrics, DEFAULT_SCHEMA)
    }

    #[cfg(test)]
    pub(crate) fn new_with_schema(
        config: &Config,
        metrics: Arc<Metrics>,
        schema: &str,
    ) -> Result<Pool, PoolError> {
        Self::new_inner(config, metrics, schema)
    }

    fn new_inner(config: &Config, metrics: Arc<Metrics>, schema: &str) -> Result<Pool, PoolError> {
        use sqlx::{pool, postgres};

        let pg_options = config
            .database_url
            .parse::<postgres::PgConnectOptions>()
            .map_err(PoolError::InvalidDatabaseUrl)?
            .ssl_mode(postgres::PgSslMode::Disable);
        let mut pool_options = pool::PoolOptions::new()
            .max_connections(config.max_pool_size)
            .min_connections(config.min_pool_idle);

        // TODO: this looks sketchy
        if schema != DEFAULT_SCHEMA {
            let search_path = Arc::from(
                format!("SET search_path TO {}, {};", schema, DEFAULT_SCHEMA).into_boxed_str(),
            );
            pool_options = pool_options.after_connect(move |conn: &mut sqlx::PgConnection| {
                let cloned = Arc::clone(&search_path);
                Box::pin(async move { conn.execute(&*cloned).await.map(|_| ()) })
            });
        }

        let pool = pool_options
            .connect_with(pg_options)
            .block()
            .map_err(PoolError::PoolCreationFailed)?;

        Ok(Pool {
            pool,
            metrics,
            max_size: config.max_pool_size,
        })
    }

    // TODO: don't return `Result`
    // TODO: can we return a PoolConnection instead? Then `&mut db.get()` would work fine.
    pub fn get(&self) -> Result<Client, PoolError> {
        self.pool.acquire().block().map_err(PoolError::ClientError)
    }

    pub(crate) fn used_connections(&self) -> u32 {
        let total_connections: u32 = unimplemented!();
        total_connections - self.idle_connections()
    }

    pub(crate) fn idle_connections(&self) -> u32 {
        unimplemented!()
    }

    pub(crate) fn max_size(&self) -> u32 {
        self.max_size
    }
}

#[derive(Debug, failure::Fail)]
pub enum PoolError {
    #[fail(display = "the provided database URL was not valid")]
    InvalidDatabaseUrl(#[fail(cause)] sqlx::Error),

    #[fail(display = "failed to create the database connection pool")]
    PoolCreationFailed(#[fail(cause)] sqlx::Error),

    #[fail(display = "failed to get a database connection")]
    ClientError(#[fail(cause)] sqlx::Error),
}
