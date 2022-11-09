use crate::common::error::Result;
use crate::common::relation::Row;
use crate::common::scalar::Datum;
use crate::sql::context::ExecutionContext;
use crate::sql::physical_plan::RowStream;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct EmptyExec {
    index: usize,
}

impl EmptyExec {
    pub fn new() -> Self {
        Self { index: 0 }
    }
}

impl EmptyExec {
    pub fn stream(&self, _exec_ctx: Arc<ExecutionContext>) -> Result<RowStream> {
        Ok(Box::pin(FilterExecStream { index: 0 }))
    }
}

struct FilterExecStream {
    index: usize,
}

impl Stream for FilterExecStream {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index == 1 {
            return Poll::Ready(None);
        }
        self.index += 1;
        let row = Row::new(vec![Datum::Null]);
        Poll::Ready(Some(Ok(row)))
    }
}
