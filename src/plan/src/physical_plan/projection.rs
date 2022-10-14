use crate::context::ExprContext;
use crate::{Expr, PhysicalPlan};
use common::error::Result;
use common::relation::{RelationDescRef, Row};

#[derive(Debug)]
pub struct ProjectionExec<'a, 'b> {
    pub expr: Vec<Expr>,
    pub ecx: &'a ExprContext<'b>,
    pub input: Box<PhysicalPlan<'a, 'b>>,
    pub rel: RelationDescRef,
}

impl<'a, 'b> ProjectionExec<'a, 'b> {
    pub fn next(&mut self) -> Result<Option<Row>> {
        let row = self.input.next()?;
        if let Some(row) = row {
            let values: Result<Vec<_>> = self
                .expr
                .iter()
                .map(|x| x.evaluate(self.ecx, &row))
                .collect();
            Ok(Some(Row::new(values?)))
        } else {
            Ok(None)
        }
    }
}
