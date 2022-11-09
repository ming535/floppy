use crate::catalog::CatalogStore;
use crate::common::relation::RelationDesc;
use crate::common::scalar::{Datum, ScalarType};
use crate::storage::TableStore;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StatementContext {
    pub catalog: Arc<dyn CatalogStore>,
    /// The types of the parameters in the query. This is
    /// filled in as planning occurs.
    pub param_types: RefCell<BTreeMap<usize, ScalarType>>,
    /// The datums of the parameters in the query. This is
    /// filled in as Binding occurs.
    pub param_values: RefCell<BTreeMap<usize, Datum>>,
}

impl StatementContext {
    pub fn new(catalog: Arc<dyn CatalogStore>) -> Self {
        Self {
            catalog,
            param_types: RefCell::default(),
            param_values: RefCell::default(),
        }
    }
}

/// A bundle of things that are needed for planning
/// `ScalarExpr`s.
#[derive(Debug, Clone)]
pub struct ExprContext {
    pub scx: Arc<StatementContext>,
    pub rel_desc: Arc<RelationDesc>,
}

impl ExprContext {
    pub fn param_types(&self) -> &RefCell<BTreeMap<usize, ScalarType>> {
        &self.scx.param_types
    }

    pub fn param_values(&self) -> &RefCell<BTreeMap<usize, Datum>> {
        &self.scx.param_values
    }
}

/// A bundle of things that is needed when execute a query.
pub struct ExecutionContext {
    pub catalog_store: Arc<dyn CatalogStore>,
    pub table_store: Arc<dyn TableStore>,
}

impl ExecutionContext {
    pub fn new(catalog_store: Arc<dyn CatalogStore>, table_store: Arc<dyn TableStore>) -> Self {
        Self {
            catalog_store,
            table_store,
        }
    }
}
