use crate::common::error::Result;
use crate::common::schema::{Schema, SchemaRef};
use std::sync::Arc;

pub trait SchemaRepo {
    fn get_schema(
        &self,
        table_name: &str,
    ) -> Result<SchemaRef>;

    fn insert_schema(&self, schema: &Schema) -> Result<()>;
}

pub struct Catalog;

impl Catalog {
    pub fn empty() -> Self {
        Catalog {}
    }
}

impl SchemaRepo for Catalog {
    fn get_schema(
        &self,
        table_name: &str,
    ) -> Result<SchemaRef> {
        todo!()
    }

    fn insert_schema(&self, schema: &Schema) -> Result<()> {
        todo!()
    }
}

pub type CatalogRef = Arc<Catalog>;
