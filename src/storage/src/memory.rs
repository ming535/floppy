use crate::{RowIter, TableStore};
use common::error::Result;
use common::relation::{GlobalId, IndexKeyDatums, IndexRange, RelationDesc, Row};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Mutex;

#[derive(Debug)]
pub struct MemoryEngine {
    rel_desc: RelationDesc,
    // Clustered table data that is sorted by primary key.
    inner: EngineInner,
}

impl MemoryEngine {
    pub fn new(rel_desc: RelationDesc) -> Self {
        Self {
            rel_desc,
            inner: EngineInner::default(),
        }
    }
}

#[derive(Debug, Default)]
struct EngineInner(Mutex<BTreeMap<IndexKeyDatums, Row>>);

impl TableStore for MemoryEngine {
    fn primary_index_range(&self, _: &GlobalId, index_range: &IndexRange) -> Result<RowIter> {
        let index_range = index_range.clone();
        let result_set = self
            .inner
            .0
            .lock()
            .unwrap()
            .clone()
            .into_iter()
            .filter(move |e| index_range.clone().contains(&e.0))
            .map(|e| Ok(e.1));

        Ok(Box::new(result_set))
    }

    fn insert(&self, _: &GlobalId, row: &Row) -> Result<()> {
        let key_datums = row.prim_key_datums(&self.rel_desc)?;
        self.inner.0.lock().unwrap().insert(key_datums, row.clone());
        Ok(())
    }
}

impl MemoryEngine {
    pub fn seed<'a, R>(&self, table_id: &GlobalId, rows: R) -> Result<()>
    where
        R: IntoIterator<Item = &'a Row>,
    {
        let row_iter = rows.into_iter();
        for r in row_iter {
            self.insert(table_id, r)?;
        }
        Ok(())
    }
}
