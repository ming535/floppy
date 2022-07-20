use crate::common::error::Result;
use crate::common::schema::SchemaRef;
use crate::common::tuple::Tuple;
use crate::physical_expr::expr::PhysicalExpr;
use crate::physical_plan::{
    SendableTupleStream, TupleStream,
};
use crate::store::{HeapStore, TupleIter};
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// todo what's the difference???

// pub struct HeapScanExec<'a, S: HeapStore> {
//     pub heap_store: &'a S,
//     pub table_name: String,
//     pub projected_schema: SchemaRef,
//     pub filters: Vec<PhysicalExpr>,
// }
//
// impl<'a, S: HeapStore> HeapScanExec<'a, S> {}

// pub struct HeapScanExec<'a> {
//     // pub heap_store: Box<dyn HeapStore>,
//     pub heap_store: &'a HeapStore,
//     pub table_name: String,
//     pub projected_schema: SchemaRef,
//     pub filters: Vec<PhysicalExpr>,
// }

#[derive(Clone)]
pub struct HeapScanExec {
    pub heap_store: Arc<dyn HeapStore>,
    pub table_name: String,
    pub projected_schema: SchemaRef,
    // todo why not Vec<PhysicalExpr>?
    pub filters: Vec<Arc<PhysicalExpr>>,
}

impl HeapScanExec {
    pub fn execute(&self) -> Result<SendableTupleStream> {
        Ok(Box::pin(HeapStream {
            heap_store: self.heap_store.clone(),
            schema: self.projected_schema.clone(),
            table_name: self.table_name.clone(),
            filters: self.filters.clone(),
            tuple_iter: self
                .heap_store
                .scan_heap(self.table_name.as_str())?,
        }))
    }
}

pub struct HeapStream {
    heap_store: Arc<dyn HeapStore>,
    schema: SchemaRef,
    table_name: String,
    filters: Vec<Arc<PhysicalExpr>>,
    tuple_iter: TupleIter,
}

impl Stream for HeapStream {
    type Item = Result<Tuple>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let tuple = self.tuple_iter.next();
        Poll::Ready(tuple)
    }
}

impl TupleStream for HeapStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
