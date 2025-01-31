use anyhow::{anyhow, bail};
use std::sync::Arc;

use datafusion::{execution::SendableRecordBatchStream, prelude::SessionContext, sql::sqlparser};

#[async_trait::async_trait]
pub trait Engine {
    async fn execute(&mut self, query: &str) -> anyhow::Result<SendableRecordBatchStream>;
}

pub struct CsvbCore {
    context: SessionContext,
}

impl CsvbCore {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        sources: Vec<String>,
        memory_limit_bytes: usize,
    ) -> anyhow::Result<Box<dyn Engine>> {
        use datafusion::prelude::*;
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let mut rt_builder = datafusion::execution::runtime_env::RuntimeEnvBuilder::new();
        rt_builder = rt_builder.with_memory_pool(Arc::new(
            datafusion::execution::memory_pool::GreedyMemoryPool::new(memory_limit_bytes),
        ));

        let runtime_env = rt_builder.build_arc()?;
        let context = SessionContext::new_with_config_rt(session_config, runtime_env);
        let csv_format = datafusion::datasource::file_format::csv::CsvFormat::default();
        let listing_options =
            datafusion::datasource::listing::ListingOptions::new(Arc::new(csv_format))
                .with_file_extension(".csv");

        let table_paths: Vec<_> = sources
            .iter()
            .map(datafusion::datasource::listing::ListingTableUrl::parse)
            .collect::<Result<_, _>>()
            .map_err(|err| anyhow!("{err}"))?;
        let resolved_schema = listing_options
            .infer_schema(&context.state(), &table_paths[0])
            .await?;
        let config =
            datafusion::datasource::listing::ListingTableConfig::new_with_multi_paths(table_paths)
                .with_listing_options(listing_options)
                .with_schema(resolved_schema);
        let listing_table = datafusion::datasource::listing::ListingTable::try_new(config)?;

        context.register_table("tbl", Arc::new(listing_table))?;

        Ok(Box::new(CsvbCore { context }))
    }
}

#[async_trait::async_trait]
impl Engine for CsvbCore {
    async fn execute(&mut self, query: &str) -> anyhow::Result<SendableRecordBatchStream> {
        Ok(self.context.sql(query).await?.execute_stream().await?)
    }
}
