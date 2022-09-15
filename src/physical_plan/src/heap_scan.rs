use common::error::Result;
use common::relation::Row;
use common::relation::{RelationDesc, RelationDescRef};
use physical_expr::expr::PhysicalExpr;

use storage::{HeapStore, RowIter};

use std::sync::Arc;

// todo what's the difference???

// pub struct HeapScanExec<'a, S: HeapStore> {
//     pub heap_store: &'a S,
//     pub table_name: String,
//     pub projected_rel: SchemaRef,
//     pub filters: Vec<PhysicalExpr>,
// }
//
// impl<'a, S: HeapStore> HeapScanExec<'a, S> {}

// pub struct HeapScanExec<'a> {
//     // pub heap_store: Box<dyn HeapStore>,
//     pub heap_store: &'a HeapStore,
//     pub table_name: String,
//     pub projected_rel: SchemaRef,
//     pub filters: Vec<PhysicalExpr>,
// }

pub struct HeapScanExec {
    pub heap_store: Arc<dyn HeapStore>,
    pub table_name: String,
    pub projected_rel: RelationDescRef,
    // todo why not Vec<PhysicalExpr>?
    pub filters: Vec<Arc<PhysicalExpr>>,

    iter: RowIter,
}

impl HeapScanExec {
    pub fn try_new(
        heap_store: Arc<dyn HeapStore>,
        table_name: String,
        projected_rel: RelationDescRef,
        filters: Vec<Arc<PhysicalExpr>>,
    ) -> Result<Self> {
        Ok(Self {
            heap_store: heap_store.clone(),
            table_name: table_name.clone(),
            projected_rel,
            filters,
            iter: heap_store.scan_heap(table_name.as_str())?,
        })
    }
}

impl HeapScanExec {
    pub fn next(&mut self) -> Result<Option<Row>> {
        self.iter.next().transpose()
    }
}
