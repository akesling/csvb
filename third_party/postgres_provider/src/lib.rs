use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use datafusion::catalog::TableProvider;
use url::Url;

pub use bb8;
pub use bb8_postgres;

pub mod postgres_connection;
pub mod postgres_pool;

pub use postgres_connection::PostgresConnection;
pub use postgres_pool::PostgresConnectionPool;

type PoolHandle = Arc<
    dyn datafusion_table_providers::sql::db_connection_pool::DbConnectionPool<
            bb8::PooledConnection<
                'static,
                bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>,
            >,
            &'static (dyn bb8_postgres::tokio_postgres::types::ToSql + Sync),
        > + Send
        + Sync,
>;

pub async fn new_postgres_pool(
    params: HashMap<String, secrecy::Secret<String>>,
) -> anyhow::Result<PoolHandle> {
    Ok(Arc::new(
        PostgresConnectionPool::new(params)
            .await
            .map_err(|error| anyhow!("{error}"))?,
    ))
}

pub async fn new_postgres_provider(
    table_name: &str,
    params: HashMap<String, String>,
) -> anyhow::Result<Arc<dyn TableProvider>> {
    let postgres_params = datafusion_table_providers::util::secrets::to_secret_map(params);
    let postgres_pool = new_postgres_pool(postgres_params).await?;
    Ok(Arc::new(
        datafusion_table_providers::sql::sql_provider_datafusion::SqlTable::new(
            "postgres",
            &postgres_pool,
            datafusion::sql::TableReference::bare(table_name),
            Some(datafusion_table_providers::sql::sql_provider_datafusion::Engine::Postgres),
        )
        .await?,
    ))
}

pub fn parse_postgres_conn_str(conn_str: &str) -> anyhow::Result<HashMap<String, String>> {
    let url = Url::parse(conn_str)?;
    let mut map = HashMap::new();

    map.insert("scheme".to_string(), url.scheme().to_string());

    let user = url.username();
    if !user.is_empty() {
        map.insert("user".to_string(), user.to_string());
    }

    if let Some(password) = url.password() {
        map.insert("password".to_string(), password.to_string());
    }

    if let Some(host) = url.host_str() {
        map.insert("host".to_string(), host.to_string());
    }

    if let Some(port) = url.port() {
        map.insert("port".to_string(), port.to_string());
    }

    // Extract the first path segment as the database name.
    // Note: the path is provided with a leading '/', so path_segments() will omit it.
    if let Some(dbname) = url.path_segments().and_then(|mut segments| segments.next()) {
        // Only add the database name if it's non-empty.
        if !dbname.is_empty() {
            map.insert("dbname".to_string(), dbname.to_string());
        }
    }

    for (key, value) in url.query_pairs() {
        map.insert(key.to_string(), value.to_string());
    }

    Ok(map)
}
