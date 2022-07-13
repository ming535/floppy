use crate::common::error::Result;
use crate::common::schema::SchemaRef;
use crate::common::tuple::Tuple;
use crate::physical_expr::expr::PhysicalExpr;
use crate::physical_plan::plan::PhysicalPlan;
use crate::physical_plan::{
    SendableTupleStream, TupleStream,
};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct ProjectionExec {
    pub expr: Vec<Arc<PhysicalExpr>>,
    pub input: Arc<PhysicalPlan>,
    pub schema: SchemaRef,
}

impl ProjectionExec {
    pub fn execute(&self) -> Result<SendableTupleStream> {
        Ok(Box::pin(ProjectionStream {
            schema: self.schema.clone(),
            expr: self.expr.clone(),
            input: self.input.execute()?,
        }))
    }
}

pub struct ProjectionStream {
    schema: SchemaRef,
    expr: Vec<Arc<PhysicalExpr>>,
    input: SendableTupleStream,
}

impl ProjectionStream {
    fn project_tuple(
        &self,
        tuple: &Tuple,
    ) -> Result<Tuple> {
        let values: Result<Vec<_>> = self
            .expr
            .iter()
            .map(|x| x.evaluate(tuple))
            .collect();
        Ok(Tuple::new(self.schema.clone(), values?))
    }
}

impl Stream for ProjectionStream {
    type Item = Result<Tuple>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(
            |x| match x {
                Some(Ok(tuple)) => {
                    Some(self.project_tuple(&tuple))
                }
                other => other,
            },
        );
        poll
    }
}

impl TupleStream for ProjectionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
