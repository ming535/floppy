use crate::physical_plan::RowStream;
use common::error::Result;
use common::relation::Row;
use common::scalar::Datum;
use futures::Stream;
use std::pin::Pin;
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
    pub fn stream(&self) -> Result<RowStream> {
        Ok(Box::pin(FilterExecStream { index: 0 }))
    }
}

struct FilterExecStream {
    index: usize,
}

impl Stream for FilterExecStream {
    type Item = Result<Row>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.index == 1 {
            return Poll::Ready(None);
        }
        self.index += 1;
        let row = Row::new(vec![Datum::Null]);
        Poll::Ready(Some(Ok(row)))
    }
}
