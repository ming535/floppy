use crate::{RowIter, TableStore};
use common::error::{table_not_found, FloppyError, Result};
use common::relation::{GlobalId, IndexKey, IndexRange, RelationDesc, Row};
use std::cell::RefCell;

use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Default)]
pub struct MemoryEngine {
    // Clustered table data that is sorted by primary key.
    data: RefCell<HashMap<GlobalId, BTreeMap<IndexKey, Vec<Row>>>>,
}

impl TableStore for MemoryEngine {
    fn primary_index_range(
        &self,
        table_id: &GlobalId,
        rel_desc: &RelationDesc,
        range: &IndexRange,
    ) -> Result<RowIter> {
        todo!()
    }

    fn insert(&self, table_id: GlobalId, row: &Row) -> Result<()> {
        todo!()
    }
}

// impl HeapStore for MemoryEngine {
//     fn scan_heap(&self, table_name: &str) -> Result<RowIter> {
//         if let Some(rows) = self.heaps.borrow().get(table_name) {
//             Ok(Box::new(MemIter::new(rows.clone())))
//         } else {
//             Err(FloppyError::Internal(format!(
//                 "table not found: {}",
//                 table_name
//             )))
//         }
//     }
//
//     fn fetch_tuple(&self, _table_name: &str) -> Result<Row> {
//         todo!()
//     }
//
//     fn insert_to_heap(&self, table_name: &str, row: &Row) -> Result<()> {
//         self.validate_schema_exists(table_name)?;
//         self.heaps
//             .borrow_mut()
//             .entry(table_name.to_string())
//             .and_modify(|r| r.push(row.clone()));
//         Ok(())
//     }
// }

// impl MemoryEngine {
//     fn validate_schema_exists(&self, table_name: &str) -> Result<()> {
//         if self.schemas.borrow().get(table_name).is_none()
//             || self.data.borrow().get(table_name).is_none()
//         {
//             Err(table_not_found(table_name))
//         } else {
//             Ok(())
//         }
//     }
//
//     pub fn seed<'a>(
//         &self,
//         table_name: &str,
//         mut rows: impl Iterator<Item = &'a Row>,
//     ) -> Result<()> {
//         self.validate_schema_exists(table_name)?;
//         while let Some(r) = rows.next() {
//             self.insert_to_heap(table_name, r)?;
//         }
//         Ok(())
//     }
// }

struct MemIter {
    rows: Vec<Row>,
    idx: usize,
}

impl MemIter {
    fn new(rows: Vec<Row>) -> Self {
        Self { rows, idx: 0 }
    }
}

impl Iterator for MemIter {
    type Item = Result<Row>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.rows.len() {
            return None;
        }

        self.idx += 1;
        Some(Ok(self.rows[self.idx - 1].clone()))
    }
}
