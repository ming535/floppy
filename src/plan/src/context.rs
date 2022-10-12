use crate::expr::{RelationExpr, ScalarExpr};
use catalog::CatalogStore;
use common::relation::{ColumnRef, RelationDesc, RelationType};
use common::scalar::ScalarType;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Formatter;

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
pub struct ScalarExprContext<'a> {
    pub scx: &'a StatementContext<'a>,
    pub rel_desc: &'a RelationDesc,
}

impl<'a> ScalarExprContext<'a> {
    pub fn param_types(&self) -> &RefCell<BTreeMap<usize, ScalarType>> {
        &self.scx.param_types
    }
}
