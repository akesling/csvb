use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _};
use datafusion::{execution::SendableRecordBatchStream, prelude::SessionContext};
use url::Url;

pub struct CsvbCore {
    context: SessionContext,
}

impl CsvbCore {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(memory_limit_bytes: usize) -> anyhow::Result<CsvbCore> {
        use datafusion::prelude::*;

        let session_config = SessionConfig::new().with_information_schema(true);
        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(
                datafusion::execution::memory_pool::GreedyMemoryPool::new(memory_limit_bytes),
            ))
            .build_arc()?;

        let context = SessionContext::new_with_config_rt(session_config, runtime_env);
        Ok(CsvbCore { context })
    }

    pub async fn add_local_table(
        self,
        name: &str,
        local_sources: &[String],
    ) -> anyhow::Result<CsvbCore> {
        if local_sources.is_empty() {
            bail!("No local sources provided");
        }

        let csv_format = datafusion::datasource::file_format::csv::CsvFormat::default();
        let listing_options =
            datafusion::datasource::listing::ListingOptions::new(Arc::new(csv_format))
                .with_file_extension(".csv");

        let table_paths: Vec<_> = local_sources
            .iter()
            .map(datafusion::datasource::listing::ListingTableUrl::parse)
            .collect::<Result<_, _>>()
            .map_err(|err| anyhow!("{err}"))?;
        let resolved_schema = listing_options
            .infer_schema(&self.context.state(), &table_paths[0])
            .await?;
        let config =
            datafusion::datasource::listing::ListingTableConfig::new_with_multi_paths(table_paths)
                .with_listing_options(listing_options)
                .with_schema(resolved_schema);
        let listing_table = datafusion::datasource::listing::ListingTable::try_new(config)?;

        self.context.register_table(name, Arc::new(listing_table))?;

        Ok(self)
    }

    pub async fn execute(&mut self, query: &str) -> anyhow::Result<SendableRecordBatchStream> {
        Ok(self.context.sql(query).await?.execute_stream().await?)
    }

    pub async fn serve(
        &mut self,
        serve_address: &str,
    ) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<()>>> {
        log::debug!("Starting up a new server");
        let listener = tokio::net::TcpListener::bind(serve_address)
            .await
            .context(format!("run_server failed to bind to {serve_address}"))?;

        log::debug!("Listening to {}", serve_address);

        let factory = Arc::new(datafusion_postgres::HandlerFactory(Arc::new(
            // TODO(akesling): wrap Engine instead of using DfSessionService over the context.
            // This will give us more control and provide a better stack of abstractions.
            datafusion_postgres::DfSessionService::new(self.context.clone()),
        )));

        Ok(tokio::spawn(async move {
            loop {
                let incoming_socket = listener.accept().await.context(
                    "run_server listener failed when attempting to open incoming socket",
                )?;
                let factory_ref = factory.clone();

                tokio::spawn(async move {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    log::debug!(
                        "Starting new session for incoming socket connection. (time={now})"
                    );
                    let result =
                        pgwire::tokio::process_socket(incoming_socket.0, None, factory_ref).await;
                    log::debug!("No longer listening on socket (start-time={now})");
                    result
                });
            }
            #[allow(unreachable_code)]
            Ok(())
        }))
    }

    pub async fn add_federated_tables(
        self,
        sharded_tables: &[VirtualTable<'_>],
    ) -> anyhow::Result<CsvbCore> {
        // TODO(akesling): Build out a table provider per shard which pushes down with datafusion
        // federation and create some listingtable-like thing which delegates across the unified
        // `tbl` surfaced to the user.
        //
        let mut table_providers = BTreeMap::new();
        for virtual_table in sharded_tables {
            let mut shard_providers = vec![];
            for addr in virtual_table.shard_addrs {
                let postgres_params = datafusion_table_providers::util::secrets::to_secret_map(
                    parse_postgres_conn_str(addr)?,
                );
                let postgres_pool = Arc::new(
                datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool::new(postgres_params)
                        .await
                        .expect("unable to create Postgres connection pool"),
                );

                let table_factory =
                    datafusion_table_providers::postgres::PostgresTableFactory::new(postgres_pool);

                let provider = table_factory
                    .table_provider(datafusion::sql::TableReference::bare(virtual_table.name))
                    .await
                    .expect("to create table provider");

                shard_providers.push(provider);
            }

            let virtual_table_provider = todo!("Actually implement an aggregate table provider");
            table_providers.insert(virtual_table.name, virtual_table_provider);
        }

        //let session_state = self
        //    .context
        //    .into_state_builder()
        //    .with_optimizer_rules({
        //        let mut rules = Optimizer::new().rules;
        //        rules.push(Arc::new(FederationOptimizerRule::new()));
        //        rules
        //    })
        //    .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
        //    .build();

        //let schema_provider = MultiSchemaProvider::new(vec![sqlite_schema_provider, postgres_schema_provider]);
        //overwrite_default_schema(&state, Arc::new(schema_provider))
        //    .expect("Overwrite the default schema form the main context");
        todo!("Register virtual tables");

        // Create the session context for the main db
        //self.context = SessionContext::new_with_state(session_state);
    }
}

pub struct VirtualTable<'a> {
    pub name: &'a str,
    pub shard_addrs: &'a [String],
}

fn parse_postgres_conn_str(conn_str: &str) -> anyhow::Result<HashMap<String, String>> {
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
