use crate::plan::PhysicalPlan;
use common::error::FloppyError;
use common::error::Result;
use common::relation::Row;
use common::scalar::Datum;
use physical_expr::expr::PhysicalExpr;
use std::sync::Arc;

pub struct FilterExec {
    pub predicate: Arc<PhysicalExpr>,
    pub input: Box<PhysicalPlan>,
}

impl FilterExec {
    pub fn next(&mut self) -> Result<Option<Row>> {
        loop {
            if let Some(r) = self.input.next()? {
                let v = self.predicate.evaluate(&r)?;
                match v {
                    Datum::Boolean(Some(true)) => break Ok(Some(r)),
                    Datum::Boolean(Some(false)) => continue,
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
