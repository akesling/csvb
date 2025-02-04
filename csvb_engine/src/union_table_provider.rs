use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::{union::UnionExec, ExecutionPlan};

/// A TableProvider that unions the output of several underlying table providers.
#[derive(Debug)]
pub struct UnionTableProvider {
    /// The underlying table providers
    providers: Vec<Arc<dyn TableProvider>>,
    /// The schema of the table (assumed to be the same for all providers)
    schema: SchemaRef,
}

impl UnionTableProvider {
    pub fn new(providers: Vec<Arc<dyn TableProvider>>, schema: SchemaRef) -> Self {
        Self { providers, schema }
    }
}

#[async_trait::async_trait]
impl TableProvider for UnionTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let projected_union_schema = if let Some(indices) = projection {
            Arc::new(self.schema.project(indices)?)
        } else {
            self.schema.clone()
        };

        let mut plans = Vec::with_capacity(self.providers.len());
        for provider in &self.providers {
            let plan = provider.scan(state, projection, filters, limit).await?;
            if plan.schema().as_ref() != projected_union_schema.as_ref() {
                return Err(DataFusionError::Internal(format!(
                    "Union schema did not match shard schema: {:#?} != {:#?}",
                    plan.schema(),
                    projected_union_schema
                )));
            }
            plans.push(plan);
        }

        Ok(Arc::new(UnionExec::new(plans)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::prelude::Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let mut support: Vec<TableProviderFilterPushDown> = (0..filters.len())
            .map(|_| TableProviderFilterPushDown::Exact)
            .collect();
        for p in &self.providers {
            for (i, update) in p.supports_filters_pushdown(filters)?.iter().enumerate() {
                support[i] = max_support(support[i].clone(), update.clone());
            }
        }
        Ok(support)
    }

    #[doc = "Get the type of this table for metadata/catalog purposes."]
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        todo!("Implement table type getter")
    }
}

fn max_support(
    first: TableProviderFilterPushDown,
    second: TableProviderFilterPushDown,
) -> TableProviderFilterPushDown {
    match (first, second) {
        (TableProviderFilterPushDown::Unsupported, _)
        | (_, TableProviderFilterPushDown::Unsupported) => TableProviderFilterPushDown::Unsupported,
        (TableProviderFilterPushDown::Inexact, _) | (_, TableProviderFilterPushDown::Inexact) => {
            TableProviderFilterPushDown::Inexact
        }
        _ => TableProviderFilterPushDown::Exact,
    }
}
