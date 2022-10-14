use catalog::CatalogStore;
use common::relation::RelationDesc;
use common::scalar::ScalarType;
use std::cell::RefCell;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct StatementContext<'a> {
    pub catalog: &'a dyn CatalogStore,
    /// The types of the parameters in the query. This is filled in as planning
    /// occurs.
    pub param_types: RefCell<BTreeMap<usize, ScalarType>>,
}

impl<'a> StatementContext<'a> {
    pub fn new(catalog: &'a dyn CatalogStore) -> Self {
        Self {
            catalog,
            param_types: RefCell::default(),
        }
    }
}

/// A bundle of things that are needed for planning `ScalarExpr`s.
#[derive(Debug, Clone)]
pub struct ExprContext<'a> {
    pub scx: &'a StatementContext<'a>,
    pub rel_desc: &'a RelationDesc,
}

impl<'a> ExprContext<'a> {
    pub fn param_types(&self) -> &RefCell<BTreeMap<usize, ScalarType>> {
        &self.scx.param_types
    }
}
