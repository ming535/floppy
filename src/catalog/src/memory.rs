use crate::{
    CatalogItem, CatalogItemType, CatalogStore, FullObjectName, PartialObjectName,
    QualifiedObjectName,
};
use common::error::{CatalogError, FloppyError};
use common::relation::{GlobalId, RelationDesc};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;

/// An in-memory catalog used in tests that requires a
/// catalog.
#[derive(Debug, Default)]
pub struct MemCatalog {
    /// the key is an item's name without any qualifier.
    tables: HashMap<String, MemCatalogItem>,
}

impl CatalogStore for MemCatalog {
    fn resolve_item(
        &self,
        partial_name: &PartialObjectName,
    ) -> common::error::Result<&dyn CatalogItem> {
        if let Some(result) = self.tables.get(&partial_name.item[..]) {
            return Ok(result);
        }

        Err(FloppyError::Catalog(CatalogError::TableNotFound(format!(
            "{}",
            partial_name.item
        ))))
    }
}

impl MemCatalog {
    pub fn insert_table(&mut self, name: &str, id: GlobalId, desc: RelationDesc) {
        let mut tmp = self.tables.clone();
        tmp.insert(
            name.into(),
            MemCatalogItem::Table {
                name: name.into(),
                id,
                desc,
            },
        );
        self.tables = tmp;

        // self.tables.insert(
        //     name.into(),
        //     MemCatalogItem::Table {
        //         name: name.into(),
        //         id,
        //         desc,
        //     },
        // );
    }
}

#[derive(Debug, Clone)]
pub enum MemCatalogItem {
    Table {
        name: QualifiedObjectName,
        id: GlobalId,
        desc: RelationDesc,
    },
}

impl CatalogItem for MemCatalogItem {
    fn name(&self) -> &QualifiedObjectName {
        match &self {
            Self::Table { name, .. } => &name,
        }
    }

    fn id(&self) -> GlobalId {
        match &self {
            Self::Table { id, .. } => *id,
        }
    }

    fn oid(&self) -> u32 {
        unimplemented!()
    }

    fn desc(&self, _: &FullObjectName) -> common::error::Result<Cow<RelationDesc>> {
        match &self {
            Self::Table { desc, .. } => Ok(Cow::Borrowed(desc)),
        }
    }

    fn item_type(&self) -> CatalogItemType {
        match &self {
            Self::Table { .. } => CatalogItemType::Table,
        }
    }

    fn create_sql(&self) -> &str {
        unimplemented!()
    }
}
