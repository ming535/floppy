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
    // `heaps` is a HashMap contains all table's row.
    // The key of the HashMap is table name, while the value is
    // all the table's row.
    heaps: HashMap<String, Vec<Row>>,
    // `schemas` is a HashMap contains all table's schema.
    // The key of the HashMap is table name, while the value is
    // a table's schema.
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
        self.heaps.insert(table_name.to_string(), vec![]);
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
        if let Some(rows) = self.heaps.get(table_name) {
            Ok(Box::new(MemIter::new(rows.clone())))
        } else {
            Err(FloppyError::Internal(format!(
                "table not found: {}",
                table_name
            )))
        }
    }

    fn fetch_tuple(
        &self,
        _table_name: &str,
        _tuple_id: &RowId,
    ) -> Result<Row> {
        todo!()
    }

    fn insert_to_heap(
        &mut self,
        table_name: &str,
        row: &Row,
    ) -> Result<()> {
        self.validate_schema_exists(table_name)?;
        self.heaps
            .entry(table_name.to_string())
            .and_modify(|r| r.push(row.clone()));
        Ok(())
    }
}

impl MemoryEngine {
    fn validate_schema_exists(
        &self,
        table_name: &str,
    ) -> Result<()> {
        if self.schemas.get(table_name).is_none()
            || self.heaps.get(table_name).is_none()
        {
            Err(table_not_found(table_name))
        } else {
            Ok(())
        }
    }

    pub fn seed<'a>(
        &mut self,
        table_name: &str,
        mut rows: impl Iterator<Item = &'a Row>,
    ) -> Result<()> {
        self.validate_schema_exists(table_name)?;
        while let Some(r) = rows.next() {
            self.insert_to_heap(table_name, r)?;
        }
        Ok(())
    }
}

impl IndexStore for MemoryEngine {}

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
