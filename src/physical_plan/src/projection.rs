use crate::plan::PhysicalPlan;
use common::error::Result;
use common::relation::RelationDescRef;
use common::relation::Row;
use physical_expr::expr::PhysicalExpr;
use std::sync::Arc;

pub struct ProjectionExec {
    pub expr: Vec<Arc<PhysicalExpr>>,
    pub input: Box<PhysicalPlan>,
    pub rel: RelationDescRef,
}

impl ProjectionExec {
    pub fn next(&mut self) -> Result<Option<Row>> {
        let row = self.input.next()?;
        if let Some(row) = row {
            let values: Result<Vec<_>> =
                self.expr.iter().map(|x| x.evaluate(&row)).collect();
            Ok(Some(Row::new(values?)))
        } else {
            Ok(None)
        }
    }
}
