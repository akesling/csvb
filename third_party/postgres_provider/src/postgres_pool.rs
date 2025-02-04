/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * copyright authors of https://github.com/datafusion-contrib/datafusion-table-providers
 *
 */

use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_trait::async_trait;
use bb8::ErrorSink;
use bb8_postgres::{
    tokio_postgres::{config::Host, types::ToSql, Config},
    PostgresConnectionManager,
};
use datafusion_table_providers::util::{self, ns_lookup::verify_ns_lookup_and_tcp_connect};
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::{prelude::*, ResultExt};
use tokio_postgres;

use super::postgres_connection::PostgresConnection;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::{AsyncDbConnection, DbConnection},
    DbConnectionPool, JoinPushDown,
};

mod res {
    pub type Error = Box<dyn std::error::Error + Send + Sync>;
    pub type Result<T, E = Error> = std::result::Result<T, E>;
}
use res::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError {
        source: bb8_postgres::tokio_postgres::Error,
    },

    #[snafu(display("ConnectionPoolRunError: {source}"))]
    ConnectionPoolRunError {
        source: bb8::RunError<bb8_postgres::tokio_postgres::Error>,
    },

    #[snafu(display("Invalid parameter: {parameter_name}"))]
    InvalidParameterError { parameter_name: String },

    #[snafu(display("Could not parse {parameter_name} into a valid integer"))]
    InvalidIntegerParameterError {
        parameter_name: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("Cannot connect to PostgreSQL on {host}:{port}. Ensure that the host and port are correctly configured, and that the host is reachable."))]
    InvalidHostOrPortError {
        source: datafusion_table_providers::util::ns_lookup::Error,
        host: String,
        port: u16,
    },

    #[snafu(display("Invalid root cert path: {path}"))]
    InvalidRootCertPathError { path: String },

    #[snafu(display("Failed to read cert : {source}"))]
    FailedToReadCertError { source: std::io::Error },

    #[snafu(display("Postgres connection error: {source}"))]
    PostgresConnectionError { source: tokio_postgres::Error },

    #[snafu(display(
        "Authentication failed. Ensure that the username and password are correctly configured."
    ))]
    InvalidUsernameOrPassword { source: tokio_postgres::Error },
}

#[derive(Debug)]
pub struct PostgresConnectionPool {
    pool: Arc<bb8::Pool<PostgresConnectionManager<tokio_postgres::NoTls>>>,
    join_push_down: JoinPushDown,
}

impl PostgresConnectionPool {
    /// Creates a new instance of `PostgresConnectionPool`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        // Remove the "pg_" prefix from the keys to keep backward compatibility
        let params = util::remove_prefix_from_hashmap_keys(params, "pg_");

        let mut connection_string = String::new();
        let ssl_mode = "disable".to_string();

        if let Some(pg_connection_string) =
            params.get("connection_string").map(Secret::expose_secret)
        {
            connection_string = parse_connection_string(pg_connection_string.as_str());
        } else {
            if let Some(pg_host) = params.get("host").map(Secret::expose_secret) {
                connection_string.push_str(format!("host={pg_host} ").as_str());
            }
            if let Some(pg_user) = params.get("user").map(Secret::expose_secret) {
                connection_string.push_str(format!("user={pg_user} ").as_str());
            }
            if let Some(pg_db) = params.get("db").map(Secret::expose_secret) {
                connection_string.push_str(format!("dbname={pg_db} ").as_str());
            }
            if let Some(pg_pass) = params.get("pass").map(Secret::expose_secret) {
                connection_string.push_str(format!("password={pg_pass} ").as_str());
            }
            if let Some(pg_port) = params.get("port").map(Secret::expose_secret) {
                connection_string.push_str(format!("port={pg_port} ").as_str());
            }
        }
        connection_string.push_str(format!("sslmode={ssl_mode} ").as_str());

        let config = Config::from_str(connection_string.as_str()).context(ConnectionPoolSnafu)?;
        verify_postgres_config(&config).await?;
        test_postgres_connection(connection_string.as_str()).await?;

        let join_push_down = get_join_context(&config);

        let manager = PostgresConnectionManager::new(config, tokio_postgres::NoTls);
        let error_sink = PostgresErrorSink::new();

        let mut connection_pool_size = 10; // The BB8 default is 10
        if let Some(pg_pool_size) = params
            .get("connection_pool_size")
            .map(Secret::expose_secret)
        {
            connection_pool_size = pg_pool_size.parse().context(InvalidIntegerParameterSnafu {
                parameter_name: "pool_size".to_string(),
            })?;
        }

        let pool = bb8::Pool::builder()
            .max_size(connection_pool_size)
            .error_sink(Box::new(error_sink))
            .build(manager)
            .await
            .context(ConnectionPoolSnafu)?;

        // Test the connection
        let conn = pool.get().await.context(ConnectionPoolRunSnafu)?;
        conn.execute("SELECT 1", &[])
            .await
            .context(ConnectionPoolSnafu)?;

        Ok(PostgresConnectionPool {
            pool: Arc::new(pool.clone()),
            join_push_down,
        })
    }

    /// Returns a direct connection to the underlying database.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn connect_direct(&self) -> Result<PostgresConnection> {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_owned().await.context(ConnectionPoolRunSnafu)?;
        Ok(PostgresConnection::new(conn))
    }
}

fn parse_connection_string(pg_connection_string: &str) -> String {
    let mut connection_string = String::new();

    let str = pg_connection_string;
    let str_params: Vec<&str> = str.split_whitespace().collect();
    for param in str_params {
        let param = param.split('=').collect::<Vec<&str>>();
        if let (Some(&name), Some(&value)) = (param.first(), param.get(1)) {
            match name {
                "sslmode" => (),
                "sslrootcert" => (),
                _ => {
                    connection_string.push_str(format!("{name}={value} ").as_str());
                }
            }
        }
    }

    connection_string
}

fn get_join_context(config: &Config) -> JoinPushDown {
    let mut join_push_context_str = String::new();
    for host in config.get_hosts() {
        join_push_context_str.push_str(&format!("host={host:?},"));
    }
    if !config.get_ports().is_empty() {
        join_push_context_str.push_str(&format!("port={port},", port = config.get_ports()[0]));
    }
    if let Some(dbname) = config.get_dbname() {
        join_push_context_str.push_str(&format!("db={dbname},"));
    }
    if let Some(user) = config.get_user() {
        join_push_context_str.push_str(&format!("user={user},"));
    }

    JoinPushDown::AllowedFor(join_push_context_str)
}

async fn test_postgres_connection(connection_string: &str) -> Result<()> {
    match tokio_postgres::connect(connection_string, tokio_postgres::NoTls).await {
        Ok(_) => Ok(()),
        Err(err) => {
            if let Some(code) = err.code() {
                if *code == tokio_postgres::error::SqlState::INVALID_PASSWORD {
                    return Err(Box::new(Error::InvalidUsernameOrPassword { source: err }));
                }
            }

            Err(Box::new(Error::PostgresConnectionError { source: err }))
        }
    }
}

async fn verify_postgres_config(config: &Config) -> Result<()> {
    for host in config.get_hosts() {
        for port in config.get_ports() {
            if let Host::Tcp(host) = host {
                verify_ns_lookup_and_tcp_connect(host, *port)
                    .await
                    .context(InvalidHostOrPortSnafu { host, port: *port })?;
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct PostgresErrorSink {}

impl PostgresErrorSink {
    pub fn new() -> Self {
        PostgresErrorSink {}
    }
}

impl<E> ErrorSink<E> for PostgresErrorSink
where
    E: std::fmt::Debug,
    E: std::fmt::Display,
{
    fn sink(&self, error: E) {
        log::error!("Postgres Connection Error: {:?}", error);
    }

    fn boxed_clone(&self) -> Box<dyn ErrorSink<E>> {
        Box::new(*self)
    }
}

#[async_trait]
impl
    DbConnectionPool<
        bb8::PooledConnection<'static, PostgresConnectionManager<tokio_postgres::NoTls>>,
        &'static (dyn ToSql + Sync),
    > for PostgresConnectionPool
{
    async fn connect(
        &self,
    ) -> Result<
        Box<
            dyn DbConnection<
                bb8::PooledConnection<'static, PostgresConnectionManager<tokio_postgres::NoTls>>,
                &'static (dyn ToSql + Sync),
            >,
        >,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_owned().await.context(ConnectionPoolRunSnafu)?;
        Ok(Box::new(PostgresConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}
