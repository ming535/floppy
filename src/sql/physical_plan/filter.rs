use crate::common::error::{FloppyError, Result};
use crate::common::relation::Row;
use crate::common::scalar::Datum;
use crate::sql::context::{ExecutionContext, ExprContext};
use crate::sql::physical_plan::RowStream;
use crate::sql::{Expr, PhysicalPlan};
use std::sync::Arc;

#[derive(Debug)]
pub struct FilterExec {
    pub predicate: Expr,
    pub ecx: ExprContext,
    pub input: Box<PhysicalPlan>,
}

impl FilterExec {
    pub fn stream(&self, exec_ctx: Arc<ExecutionContext>) -> Result<RowStream> {
        todo!()
        //     loop {
        //         if let Some(r) = self.input.evaluate()? {
        //             let v =
        // self.predicate.evaluate(&self.ecx, &r)?;
        //             match v {
        //                 Datum::Boolean(true) => break
        // Ok(Some(r)),
        // Datum::Boolean(false) => continue,
        //                 other => {
        //                     break
        // Err(FloppyError::Internal(format!(
        //                         "predicate evaluate
        // error: {:?}",
        // other                     )))
        //                 }
        //             }
        //         } else {
        //             break Ok(None);
        //         }
        //     }
    }
}
