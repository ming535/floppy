use crate::common::error::Result;
use crate::common::schema::Schema;
use crate::common::tuple::{Tuple, TupleId};

/// `CatalogStore`, `HeapStore` and `IndexStore` are basic abstractions
/// for Floppy's storage engine.
/// `CatalogStore` is used to manage meta data of Floppy including schema,
/// statistics.
/// `HeapStore` is used to manage heap data. Heap data is an unsorted area that
/// holds a table's tuple.
/// `IndexStore` is used to manage index data. Index data is a sorted area that
/// holds a table's index.
pub trait CatalogStore {
    /// Insert a schema into catalog. `table_name` is a qualified name
    /// like "database_name.table_name".
    fn insert_schema(
        &mut self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<()>;

    /// Fetch schema for this table.
    fn fetch_schema(
        &self,
        table_name: &str,
    ) -> Result<Schema>;
}

pub type TupleIter =
    Box<dyn Iterator<Item = Result<Tuple>>>;

pub trait HeapStore {
    /// Returns a `TupleIter` to scan a table's heap
    fn scan_heap(
        &self,
        table_name: &str,
    ) -> Result<TupleIter>;
    /// Fetch a tuple from heap using tuple_id
    fn fetch_tuple(
        &self,
        table_name: &str,
        tuple_id: &TupleId,
    ) -> Result<Tuple>;

    /// Insert a tuple into heap
    fn insert_to_heap(
        &mut self,
        table_name: &str,
        tuple: &Tuple,
    ) -> Result<()>;
}

pub trait IndexStore {}
