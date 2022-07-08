use crate::common::error::Result;
use crate::common::schema::SchemaRef;
use crate::common::tuple::Tuple;
use crate::physical_expr::expr::PhysicalExpr;
use crate::physical_plan::plan::PhysicalPlan;
use crate::physical_plan::{SendableTupleStream, TupleStream};
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct ProjectionExec {
    pub expr: Vec<PhysicalExpr>,
    pub input: Arc<PhysicalPlan>,
    pub schema: SchemaRef,
}

impl ProjectionExec {
    pub fn execute(&self) -> Result<SendableTupleStream> {
        Ok(Box::new(ProjectionStream {
            schema: self.schema.clone(),
            expr: self.expr.clone(),
        }))
    }
}

pub struct ProjectionStream {
    schema: SchemaRef,
    expr: Vec<PhysicalExpr>,
}

impl Stream for ProjectionStream {
    type Item = Result<Tuple>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

impl TupleStream for ProjectionStream {
    fn schema(&self) -> SchemaRef {
        todo!()
    }
}
