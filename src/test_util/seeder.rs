use crate::catalog::names::{FullObjectName, PartialObjectName};
use crate::catalog::CatalogStore;
use crate::common::error::Result;
use crate::common::relation::{ColumnType, GlobalId, RelationDesc, Row};
use crate::common::scalar::ScalarType;
use crate::{catalog, storage};
use lazy_static::lazy_static;
use std::sync::Arc;

/// test
lazy_static! {
    static ref TEST_TABLE_NAME: &'static str = "test";
    static ref TEST_TABLE_ID: GlobalId = 1;
    static ref TEST_REL_DESC: RelationDesc = RelationDesc::new(
        vec![
            ColumnType::new(ScalarType::Int64, false),
            ColumnType::new(ScalarType::Int64, false),
        ],
        vec!["c1".to_string(), "c2".to_string()],
        vec![0, 1],
        vec![],
    );
}

pub fn seed_catalog() -> Arc<dyn catalog::CatalogStore> {
    let mut catalog = catalog::memory::MemCatalog::default();
    catalog.insert_table(*TEST_TABLE_NAME, *TEST_TABLE_ID, TEST_REL_DESC.clone());
    Arc::new(catalog)
}

pub fn seed_table(rel_desc: RelationDesc, data: &Vec<Row>) -> Result<Arc<dyn storage::TableStore>> {
    let mut table = Arc::new(storage::memory::MemoryEngine::new(rel_desc));
    table.seed(&TEST_TABLE_ID, data)?;
    Ok(table)
}

pub fn seed_catalog_and_table(
    data: &Vec<Row>,
) -> Result<(Arc<dyn catalog::CatalogStore>, Arc<dyn storage::TableStore>)> {
    let catalog = seed_catalog();
    let partial_name: PartialObjectName = (*TEST_TABLE_NAME).into();
    let full_name: FullObjectName = partial_name.clone().into();
    let rel_desc = catalog.resolve_item(&partial_name)?.desc(&full_name)?;
    let table = seed_table(rel_desc.into_owned(), data)?;
    Ok((catalog, table))
}
