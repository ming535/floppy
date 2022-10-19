/// The storage layer hides the details of the persistent and query of database objects.
use common::error::Result;
use common::relation::{GlobalId, IndexRange, RelationDesc, Row};
use common::scalar::Datum;
use std::fmt;
use std::ops::{Bound, RangeBounds};

pub mod memory;

pub type RowIter = Box<dyn Iterator<Item = Result<Row>>>;

pub trait TableStore {
    fn primary_index_range(
        &self,
        table_id: &GlobalId,
        rel_desc: &RelationDesc,
        range: &IndexRange,
    ) -> Result<RowIter>;

    fn full_scan(&self, table_id: &GlobalId, rel_desc: &RelationDesc) -> Result<RowIter> {
        self.primary_index_range(
            table_id,
            rel_desc,
            &IndexRange {
                lo: Bound::Unbounded,
                hi: Bound::Unbounded,
            },
        )
    }

    fn insert(&self, table_id: GlobalId, row: &Row) -> Result<()>;

    // todo! add secondary_index_scan
}
