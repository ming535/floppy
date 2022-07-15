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
    use crate::logical_expr::literal::lit;
    use crate::logical_plan::builder::LogicalPlanBuilder;
    use futures::{StreamExt, TryStreamExt};

    #[tokio::test]
    async fn test_select_no_relation() -> Result<()> {
        let builder = LogicalPlanBuilder::empty();
        let builder =
            builder.project(vec![lit(1), lit(2)])?;
        let logical_plan = builder.build()?;
        println!("LogicalPlan: {:?}", logical_plan);

        let planner = PhysicalPlanner::default();
        let physical_plan =
            planner.create_physical_plan(&logical_plan)?;
        println!("PhysicalPlan: {:?}", physical_plan);
        let mut stream = physical_plan.execute()?;
        let data = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(FloppyError::from)?;
        // let tuple = stream.next();
        // let t = tuple.await;
        // let t = t.unwrap()?;
        println!("t = {:?}", data);
        // let result = stream.collect::<Vec<_>>();
        // println!("result = {:?}", result);

        Ok(())
    }
}
