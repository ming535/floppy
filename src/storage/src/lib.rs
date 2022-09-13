use common::error::Result;
use common::row::{Row, RowId};
use common::schema::Schema;

pub mod memory;

/// `CatalogStore`, `HeapStore` and `IndexStore` are basic abstractions
/// for Floppy's storage engine.
/// `CatalogStore` is used to manage meta data of Floppy including schema,
/// statistics.
/// `HeapStore` is used to manage heap data. Heap data is an unsorted area that
/// holds a table's tuple.
/// `IndexStore` is used to manage index data. Index data is a sorted area that
/// holds a table's index.
///
/// Note that traits that modify store is defined as immutable method, so
/// implementations of trait should enforce the borrow rule at runtime.
pub trait CatalogStore {
    /// Insert a schema into catalog. `table_name` is a qualified name
    /// like "database_name.table_name".
    fn insert_schema(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<()>;

    /// Fetch schema for this table.
    fn fetch_schema(
        &self,
        table_name: &str,
    ) -> Result<Schema>;
}

pub type RowIter = Box<dyn Iterator<Item = Result<Row>>>;

pub trait HeapStore {
    /// Returns a `TupleIter` to scan a table's heap
    fn scan_heap(
        &self,
        table_name: &str,
    ) -> Result<RowIter>;
    /// Fetch a tuple from heap using tuple_id
    fn fetch_tuple(
        &self,
        table_name: &str,
        tuple_id: &RowId,
    ) -> Result<Row>;

    /// Insert a tuple into heap
    fn insert_to_heap(
        &self,
        table_name: &str,
        tuple: &Row,
    ) -> Result<()>;
}

pub trait Store: CatalogStore + HeapStore {}

pub trait IndexStore {}