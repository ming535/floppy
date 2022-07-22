use crate::common::error::{
    table_not_found, FloppyError, Result,
};
use crate::common::row::{Row, RowId};
use crate::common::schema::Schema;
use crate::store::{
    CatalogStore, HeapStore, IndexStore, RowIter,
};
use std::collections::HashMap;

#[derive(Default)]
pub struct MemoryEngine {
    heaps: HashMap<String, Vec<Row>>,
    schemas: HashMap<String, Schema>,
}

impl CatalogStore for MemoryEngine {
    fn insert_schema(
        &mut self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<()> {
        self.schemas
            .insert(table_name.to_string(), schema.clone());
        Ok(())
    }

    fn fetch_schema(
        &self,
        table_name: &str,
    ) -> Result<Schema> {
        let schema = self.schemas.get(table_name);
        match schema {
            Some(s) => Ok(s.clone()),
            None => Err(table_not_found(table_name)),
        }
    }
}

impl HeapStore for MemoryEngine {
    fn scan_heap(
        &self,
        table_name: &str,
    ) -> Result<RowIter> {
        todo!()
    }

    fn fetch_tuple(
        &self,
        table_name: &str,
        tuple_id: &RowId,
    ) -> Result<Row> {
        todo!()
    }

    fn insert_to_heap(
        &mut self,
        table_name: &str,
        tuple: &Row,
    ) -> Result<()> {
        todo!()
    }
}

impl IndexStore for MemoryEngine {}
