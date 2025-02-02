use anyhow::{anyhow, Context as _};
use std::sync::Arc;

use datafusion::{execution::SendableRecordBatchStream, prelude::SessionContext};

pub struct CsvbCore {
    context: SessionContext,
}

impl CsvbCore {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        local_sources: &[String],
        memory_limit_bytes: usize,
    ) -> anyhow::Result<CsvbCore> {
        use datafusion::prelude::*;
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let mut rt_builder = datafusion::execution::runtime_env::RuntimeEnvBuilder::new();
        rt_builder = rt_builder.with_memory_pool(Arc::new(
            datafusion::execution::memory_pool::GreedyMemoryPool::new(memory_limit_bytes),
        ));

        let runtime_env = rt_builder.build_arc()?;
        let context = SessionContext::new_with_config_rt(session_config, runtime_env);

        if !local_sources.is_empty() {
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
                .infer_schema(&context.state(), &table_paths[0])
                .await?;
            let config = datafusion::datasource::listing::ListingTableConfig::new_with_multi_paths(
                table_paths,
            )
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
            let listing_table = datafusion::datasource::listing::ListingTable::try_new(config)?;

            context.register_table("tbl", Arc::new(listing_table))?;
        }

        Ok(CsvbCore { context })
    }

    pub async fn execute(&mut self, query: &str) -> anyhow::Result<SendableRecordBatchStream> {
        Ok(self.context.sql(query).await?.execute_stream().await?)
    }

    pub async fn serve_local_data(
        &mut self,
        serve_address: &str,
    ) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<()>>> {
        log::debug!("Starting up a new server");
        let listener = tokio::net::TcpListener::bind(serve_address)
            .await
            .context(format!("run_server failed to bind to {serve_address}"))?;

        log::info!("Listening to {}", serve_address);

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

    pub async fn serve_federated_data(
        &mut self,
        _serve_address: &str,
        _sharded_tables: &[VirtualTable<'_>],
    ) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<()>>> {
        // TODO(akesling): Build out a table provider per shard which pushes down with datafusion
        // federation and create some listingtable-like thing which delegates across the unified
        // `tbl` surfaced to the user.
        todo!()
    }
}

pub struct VirtualTable<'a> {
    pub name: &'a str,
    pub shard_addrs: &'a [String],
}
