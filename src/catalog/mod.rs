use crate::common::error::Result;
use crate::common::schema::{Schema, SchemaRef};
use std::sync::Arc;

pub struct Catalog;

impl Catalog {
    pub fn empty() -> Self {
        Catalog {}
    }

    pub fn get_schema(&self, table_name: &str) -> Result<SchemaRef> {
        todo!()
    }
}

pub type CatalogRef = Arc<Catalog>;
