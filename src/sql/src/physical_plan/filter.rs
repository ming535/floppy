use crate::context::{ExecutionContext, ExprContext};
use crate::physical_plan::RowStream;
use crate::{Expr, PhysicalPlan};
use common::error::{FloppyError, Result};
use common::relation::Row;
use common::scalar::Datum;
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
