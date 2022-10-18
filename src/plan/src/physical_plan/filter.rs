use crate::context::ExprContext;
use crate::{Expr, PhysicalPlan};
use common::error::{FloppyError, Result};
use common::relation::Row;
use common::scalar::Datum;

#[derive(Debug)]
pub struct FilterExec {
    pub predicate: Expr,
    pub ecx: ExprContext,
    pub input: Box<PhysicalPlan>,
}

impl FilterExec {
    pub fn next(&mut self) -> Result<Option<Row>> {
        loop {
            if let Some(r) = self.input.next()? {
                let v = self.predicate.evaluate(&self.ecx, &r)?;
                match v {
                    Datum::Boolean(true) => break Ok(Some(r)),
                    Datum::Boolean(false) => continue,
                    other => {
                        break Err(FloppyError::Internal(format!(
                            "predicate evaluate error: {:?}",
                            other
                        )))
                    }
                }
            } else {
                break Ok(None);
            }
        }
    }
}
