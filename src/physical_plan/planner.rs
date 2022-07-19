use crate::common::error::{FloppyError, Result};
use crate::common::schema::Schema;
use crate::logical_expr::expr::LogicalExpr;
use crate::logical_plan::plan::{
    Filter, LogicalPlan, Projection,
};
use crate::physical_expr::binary_expr::binary;
use crate::physical_expr::column::Column;
use crate::physical_expr::expr::PhysicalExpr;
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::plan::{
    PhysicalPlan, TableScanExec,
};
use crate::physical_plan::projection::ProjectionExec;
use std::sync::Arc;

#[derive(Default)]
pub struct PhysicalPlanner {}

impl PhysicalPlanner {
    fn create_physical_expr(
        &self,
        expr: &LogicalExpr,
        schema: &Schema,
    ) -> Result<Arc<PhysicalExpr>> {
        match expr {
            LogicalExpr::Column(c) => {
                let idx = schema.index_of_column(&c)?;
                Ok(Arc::new(PhysicalExpr::Column(Column {
                    name: c.name.clone(),
                    index: idx,
                })))
            }
            LogicalExpr::Literal(v) => Ok(Arc::new(
                PhysicalExpr::Literal(v.clone()),
            )),
            LogicalExpr::BinaryExpr { left, op, right } => {
                let lhs = self
                    .create_physical_expr(left, schema)?;
                let rhs = self
                    .create_physical_expr(right, schema)?;
                binary(lhs, *op, rhs, schema)
            }
        }
    }

    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<PhysicalPlan> {
        match logical_plan {
            LogicalPlan::EmptyRelation(empty) => {
                Ok(PhysicalPlan::EmptyExec(EmptyExec {
                    schema: empty.schema.clone(),
                }))
            }
            LogicalPlan::TableScan(scan) => {
                Ok(PhysicalPlan::TableScanExec(
                    TableScanExec {},
                ))
            }
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
            }) => {
                let exprs = expr
                    .iter()
                    .map(|e| {
                        self.create_physical_expr(e, schema)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let input =
                    self.create_physical_plan(input)?;
                Ok(PhysicalPlan::ProjectionExec(
                    ProjectionExec {
                        expr: exprs,
                        input: Arc::new(input),
                        schema: schema.clone(),
                    },
                ))
            }
            LogicalPlan::Filter(Filter {
                predicate,
                input,
            }) => {
                todo!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::schema::{DataType, Field};
    use crate::common::tuple::Tuple;
    use crate::common::value::Value;
    use crate::logical_expr::literal::lit;
    use crate::logical_plan::builder::LogicalPlanBuilder;
    use crate::storage::memory::MemoryEngine;
    use crate::store::CatalogStore;
    use futures::{StreamExt, TryStreamExt};

    #[tokio::test]
    async fn test_select_no_relation() -> Result<()> {
        let builder = LogicalPlanBuilder::empty_relation();
        let builder =
            builder.project(vec![lit(1), lit(2)])?;
        let logical_plan = builder.build()?;

        let planner = PhysicalPlanner::default();
        let physical_plan =
            planner.create_physical_plan(&logical_plan)?;
        assert_eq!(format!("{}", physical_plan), 
                   "ProjectionExec: Literal(Int64(1)), Literal(Int64(2))\
                  \n  EmptyExec");
        let mut stream = physical_plan.execute()?;
        let data = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(FloppyError::from)?;
        assert_eq!(data.len(), 1);
        assert_eq!(
            data[0],
            Tuple::new(vec![
                Value::Int64(Some(1)),
                Value::Int64(Some(2))
            ])
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_simple_scan() -> Result<()> {
        let table_name = "test";

        let mut mem_engine = MemoryEngine::default();
        let test_schema = Schema::new(vec![Field::new(
            Some(table_name),
            "id",
            DataType::Int32,
            false,
        )]);
        let r =
            mem_engine.insert_schema("test", &test_schema);
        if r.is_err() {
            return Err(FloppyError::Internal(
                "h".to_string(),
            ));
        }

        let logical_plan_builder =
            LogicalPlanBuilder::scan(
                table_name,
                Arc::new(
                    mem_engine
                        .fetch_schema(table_name)
                        .unwrap(),
                ),
                vec![],
            )?;

        let planner = PhysicalPlanner::default();
        let physical_plan = planner.create_physical_plan(
            &logical_plan_builder.build()?,
        )?;

        assert_eq!(
            format!("{}", physical_plan),
            "TableScanExec"
        );
        Ok(())
    }
}
