use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _};
use datafusion::datasource::TableProvider;
use datafusion::{execution::SendableRecordBatchStream, prelude::SessionContext};
use url::Url;

mod union_table_provider;
use union_table_provider::UnionTableProvider;

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
        let mut table_providers = BTreeMap::new();
        for virtual_table in sharded_tables {
            let mut shard_providers: Vec<Arc<dyn TableProvider>> = vec![];
            for addr in virtual_table.shard_addrs {
                log::trace!(
                    "Assembling shard table provider for virtual table '{}' w/ address '{}'",
                    virtual_table.name,
                    addr
                );

                let parsed = postgres_provider::parse_postgres_conn_str(addr)?;
                log::trace!("Parsing '{addr}' resulted in '{parsed:?}'");

                log::trace!("Building provider itself");
                let provider =
                    postgres_provider::new_postgres_provider(virtual_table.name, parsed).await?;

                shard_providers.push(provider);
            }

            // Assert that all shard providers have equivalent schema
            let schema = shard_providers
                .first()
                .ok_or(anyhow!("No shard providers were found"))?
                .schema();
            for provider in &shard_providers {
                if provider.schema() != schema {
                    bail!("Schema of shards was not identical")
                }
            }

            log::trace!(
                "Assembling UnionTableProvider for table {}",
                virtual_table.name
            );
            let virtual_table_provider = UnionTableProvider::new(shard_providers, schema);
            table_providers.insert(virtual_table.name, virtual_table_provider);
        }

        for (name, provider) in table_providers {
            if self
                .context
                .register_table(name, Arc::new(provider))?
                .is_some()
            {
                bail!("Table provider registered multiple times for table '{name}'");
            }
        }
        Ok(self)
    }
}

pub struct VirtualTable<'a> {
    pub name: &'a str,
    pub shard_addrs: &'a [String],
}
