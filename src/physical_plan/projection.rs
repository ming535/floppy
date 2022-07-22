use crate::common::error::Result;
use crate::common::row::Row;
use crate::common::schema::SchemaRef;
use crate::physical_expr::expr::PhysicalExpr;
use crate::physical_plan::plan::PhysicalPlan;
use crate::physical_plan::{
    SendableTupleStream, TupleStream,
};
use futures::{Stream, StreamExt};
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct ProjectionExec {
    pub expr: Vec<Arc<PhysicalExpr>>,
    pub input: Box<PhysicalPlan>,
    pub schema: SchemaRef,
}

impl ProjectionExec {
    pub fn next(&mut self) -> Result<Option<Row>> {
        let row = self.input.next()?;
        if let Some(row) = row {
            let values: Result<Vec<_>> = self
                .expr
                .iter()
                .map(|x| x.evaluate(&row))
                .collect();
            Ok(Some(Row::new(values?)))
        } else {
            Ok(None)
        }
    }
}

pub struct ProjectionStream {
    schema: SchemaRef,
    expr: Vec<Arc<PhysicalExpr>>,
    input: SendableTupleStream,
}

impl ProjectionStream {
    fn project_tuple(&self, tuple: &Row) -> Result<Row> {
        let values: Result<Vec<_>> = self
            .expr
            .iter()
            .map(|x| x.evaluate(tuple))
            .collect();
        Ok(Row::new(values?))
    }
}

impl Stream for ProjectionStream {
    type Item = Result<Row>;

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
