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
    ) -> Result<Arc<PhysicalPlan>> {
        match logical_plan {
            LogicalPlan::EmptyRelation(empty) => {
                Ok(Arc::new(PhysicalPlan::EmptyExec(
                    EmptyExec {
                        schema: empty.schema.clone(),
                    },
                )))
            }
            LogicalPlan::TableScan(scan) => {
                Ok(Arc::new(PhysicalPlan::TableScanExec(
                    TableScanExec {},
                )))
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
                Ok(Arc::new(PhysicalPlan::ProjectionExec(
                    ProjectionExec {
                        expr: exprs,
                        input,
                        schema: schema.clone(),
                    },
                )))
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

    fn select_no_relation() {}
}
