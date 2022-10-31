use crate::context::{ExecutionContext, ExprContext};
use crate::physical_plan::RowStream;
use crate::{Expr, PhysicalPlan};
use common::error::Result;
use common::relation::{RelationDesc, Row};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct ProjectionExec {
    pub exprs: Vec<Expr>,
    pub ecx: ExprContext,
    pub input: Box<PhysicalPlan>,
    pub rel_desc: Arc<RelationDesc>,
}

impl ProjectionExec {
    pub fn stream(&self, exec_ctx: Arc<ExecutionContext>) -> Result<RowStream> {
        Ok(Box::pin(ProjectionExecStream {
            ecx: self.ecx.clone(),
            input: self.input.stream(exec_ctx.clone())?,
            exprs: self.exprs.clone(),
        }))
    }
}

struct ProjectionExecStream {
    exprs: Vec<Expr>,
    ecx: ExprContext,
    input: RowStream,
}

impl ProjectionExecStream {
    fn project(&self, r: &Row) -> Result<Row> {
        let values = self
            .exprs
            .iter()
            .map(|x| x.evaluate(&self.ecx, r))
            .collect::<Result<Vec<_>>>()?;
        Ok(Row::new(values))
    }
}

impl Stream for ProjectionExecStream {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(r)) => Some(self.project(&r)),
            other => other,
        })
    }
}
