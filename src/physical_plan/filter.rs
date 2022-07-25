use crate::common::error::FloppyError;
use crate::common::error::Result;
use crate::common::row::Row;
use crate::common::value::Value;
use crate::physical_expr::expr::PhysicalExpr;
use crate::physical_plan::plan::PhysicalPlan;
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
                    Value::Boolean(Some(true)) => break Ok(Some(r)),
                    Value::Boolean(Some(false)) => continue,
                    other => break Err(FloppyError::Internal(format!("predicate evaluate error: {:?}", other))),
                }
            } else {
                break Ok(None);
            }
        }
    }
}
