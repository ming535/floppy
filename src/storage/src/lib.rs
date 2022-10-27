/// The storage layer hides the details of the persistent and query of database objects.
use common::error::Result;
use common::relation::{GlobalId, IndexRange, Row};
use std::ops::{Bound, RangeBounds};

pub mod memory;

pub type RowIter = Box<dyn Iterator<Item = Result<Row>>>;

pub trait TableStore {
    fn primary_index_range(
        &self,
        table_id: &GlobalId,
        range: &IndexRange,
    ) -> Result<RowIter>;

    fn full_scan(&self, table_id: &GlobalId) -> Result<RowIter> {
        self.primary_index_range(
            table_id,
            &IndexRange {
                lo: Bound::Unbounded,
                hi: Bound::Unbounded,
            },
        )
    }

    fn insert(&self, table_id: &GlobalId, row: &Row) -> Result<()>;

    // todo! add secondary_index_scan
}
