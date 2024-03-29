use crate::catalog::names::FullObjectName;
use crate::common::error::Result;
use crate::common::relation::{GlobalId, RelationDesc, Row};
use crate::sql::context::ExecutionContext;
use crate::sql::physical_plan::RowStream;
use crate::storage::RowIter;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct PriKeyScanExec {
    pub table_id: GlobalId,
    pub rel_desc: RelationDesc,
    pub full_name: FullObjectName,
}

impl PriKeyScanExec {
    pub fn stream(&self, exec_ctx: Arc<ExecutionContext>) -> Result<RowStream> {
        let row_iter = exec_ctx.table_store.full_scan(&self.table_id)?;
        Ok(Box::pin(PriKeyScanExecStream { row_iter }))
    }
}

struct PriKeyScanExecStream {
    row_iter: RowIter,
}

impl Stream for PriKeyScanExecStream {
    type Item = Result<Row>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let row = self.row_iter.next();
        match row {
            None => Poll::Ready(None),
            Some(r) => Poll::Ready(Some(r)),
        }
    }
}
