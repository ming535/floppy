use crate::context::ExprContext;
use crate::{Expr, PhysicalPlan};
use common::error::Result;
use common::relation::{RelationDesc, Row};
use std::sync::Arc;

#[derive(Debug)]
pub struct ProjectionExec {
    pub exprs: Vec<Expr>,
    pub ecx: ExprContext,
    pub input: Box<PhysicalPlan>,
    pub rel_desc: Arc<RelationDesc>,
}

impl ProjectionExec {
    pub fn next(&mut self) -> Result<Option<Row>> {
        let row = self.input.next()?;
        if let Some(row) = row {
            let values: Result<Vec<_>> = self
                .exprs
                .iter()
                .map(|x| x.evaluate(&self.ecx, &row))
                .collect();
            Ok(Some(Row::new(values?)))
        } else {
            Ok(None)
        }
    }
}
