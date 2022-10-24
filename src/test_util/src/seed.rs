use common::error::Result;
use common::relation::{ColumnType, GlobalId, RelationDesc, Row};
use common::scalar::ScalarType;
use lazy_static::lazy_static;
use std::sync::Arc;

/// test
///
///
lazy_static! {
    static ref TEST_TABLE_NAME: &'static str = "test";
    static ref TEST_TABLE_ID: GlobalId = 1;
    static ref TEST_REL_DESC: RelationDesc = RelationDesc::new(
        vec![
            ColumnType::new(ScalarType::Int32, false),
            ColumnType::new(ScalarType::Int32, false),
        ],
        vec!["c1".to_string(), "c2".to_string()],
        vec![],
        vec![],
    );
}

pub fn seed_catalog() -> Box<dyn catalog::CatalogStore> {
    let mut catalog = Box::new(catalog::memory::MemCatalog::default());
    catalog.insert_table(*TEST_TABLE_NAME, *TEST_TABLE_ID, TEST_REL_DESC.clone());
    catalog
}

pub fn seed_table(data: &Vec<Row>) -> Result<Box<dyn storage::TableStore>> {
    let mut table = Box::new(storage::memory::MemoryEngine::default());
    table.seed(&TEST_TABLE_ID, data)?;
    Ok(table)
}

pub fn seed(
    data: &Vec<Row>,
) -> Result<(Box<dyn catalog::CatalogStore>, Box<dyn storage::TableStore>)> {
    let catalog = seed_catalog();
    let table = seed_table(data)?;
    Ok((catalog, table))
}
