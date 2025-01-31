use anyhow::{anyhow, bail};
use std::sync::Arc;

pub struct CmdOptions {
    /// The number of bytes the command memory pool should be limited to
    pub memory_limit_bytes: usize,
}

pub async fn run_cmd(options: &CmdOptions, sources: Vec<String>, sql: &str) -> anyhow::Result<()> {
    use datafusion::prelude::*;

    if sources.is_empty() {
        bail!("No sources provided when running command")
    }

    let session_config = SessionConfig::from_env()?.with_information_schema(true);
    let mut rt_builder = datafusion::execution::runtime_env::RuntimeEnvBuilder::new();
    rt_builder = rt_builder.with_memory_pool(Arc::new(
        datafusion::execution::memory_pool::GreedyMemoryPool::new(options.memory_limit_bytes),
    ));

    let runtime_env = rt_builder.build_arc()?;
    let ctx = SessionContext::new_with_config_rt(session_config, runtime_env);
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
        .infer_schema(&ctx.state(), &table_paths[0])
        .await?;
    let config =
        datafusion::datasource::listing::ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
    let listing_table = datafusion::datasource::listing::ListingTable::try_new(config)?;

    ctx.register_table("tbl", Arc::new(listing_table))?;
    // TODO(alex): Add UDF to print haiku
    //
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;

    let pretty_results = arrow::util::pretty::pretty_format_batches(&batches[..])?.to_string();
    println!("Results:\n{}", pretty_results);
    Ok(())
}
