use crate::{RowIter, TableStore};
use common::error::{table_not_found, FloppyError, Result};
use common::relation::{GlobalId, IndexKeyDatums, IndexRange, RelationDesc, Row};
use std::cell::RefCell;

use std::collections::btree_map::Range;
use std::collections::{BTreeMap, HashMap};
use std::ops::RangeBounds;
use std::rc::Rc;

#[derive(Debug, Default)]
pub struct MemoryEngine {
    rel_desc: RelationDesc,
    // Clustered table data that is sorted by primary key.
    inner: EngineInner,
}

impl MemoryEngine {
    pub fn empty(rel_desc: RelationDesc) -> Self {
        Self {
            rel_desc,
            inner: EngineInner::default(),
        }
    }
}

#[derive(Debug, Default)]
struct EngineInner(RefCell<BTreeMap<IndexKeyDatums, Row>>);

impl TableStore for MemoryEngine {
    fn primary_index_range(
        &self,
        table_id: &GlobalId,
        index_range: &IndexRange,
    ) -> Result<RowIter> {
        let index_range = index_range.clone();
        let result_set = self
            .inner
            .0
            .borrow()
            .clone()
            .into_iter()
            .filter(move |e| index_range.clone().contains(&e.0))
            .map(|e| Ok(e.1));

        Ok(Box::new(result_set))
    }

    fn insert(&self, table_id: &GlobalId, row: &Row) -> Result<()> {
        let key_datums = row.prim_key_datums(&self.rel_desc)?;
        self.inner.0.borrow_mut().insert(key_datums, row.clone());
        Ok(())
    }
}
