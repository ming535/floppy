pub mod memory;
pub mod names;

use crate::names::{
    DatabaseId, FullObjectName, PartialObjectName, QualifiedObjectName, SchemaId,
};
use common::error::Result;
use common::relation::{GlobalId, RelationDesc};
use std::borrow::Cow;
use std::fmt;

/// To simplify the design, all SQL objects are under "fp" database.
const FLOPPY_DB_NAME: &str = "fp";
const FLOPPY_DB_ID: DatabaseId = DatabaseId(100);

/// To simplify the design, all user tables are under "public" schema.
const FLOPPY_SCHEMA_NAME: &str = "public";
const FLOPPY_SCHEMA_ID: SchemaId = SchemaId(101);

/// A catalog keeps track of SQL objects available to the planner.
///
/// The SQL standard mandates a catalog hierarchy of exactly three layers.
/// A catalog contains databases, database contain schemas, and schemas contain
/// catalog items, like table, indexes.
///
/// There are several reasons one might want to use schemas:
///
/// https://www.postgresql.org/docs/current/ddl-schemas.html
///
/// To simplify the design of Floppy, all SQL objects are put into the database
/// of "test", and all use created tables are in the "public" schema (the "pg_catalog"
/// schema contains the system tables and all the built-in data types, functions,
/// and operators.).
///
/// There are two classes of operations provided by a catalog:
///   * Resolution operations, like [`resolve_item`]. These fill in missing name
///     components based upon connection defaults, e.g., resolving the partial
///     name `test_table` to the fully-specified name `test.public.test_table`.
///
///   * Lookup operations, like [`get_item`]. These retrieve
///     metadata about a catalog entity based on a fully-specified name that is
///     known to be valid (i.e., because the name was successfully resolved,
///     or was constructed based on the output of a prior lookup operation).
pub trait CatalogStore: fmt::Debug {
    fn resolve_item(&self, item_name: &PartialObjectName) -> Result<&dyn CatalogItem>;
}

impl<C: CatalogStore + ?Sized> CatalogStore for Box<C> {
    fn resolve_item(&self, item_name: &PartialObjectName) -> Result<&dyn CatalogItem> {
        (**self).resolve_item(item_name)
    }
}

/// An item in a [`CatalogStore`].
///
/// "item" has a very specific meaning in the context of a SQL
/// catalog, and refers to the various entities that belong to a schema.
pub trait CatalogItem {
    /// Returns the fully qualified name of the catalog item.
    fn name(&self) -> &QualifiedObjectName;

    /// Returns a stable ID for the catalog item.
    fn id(&self) -> GlobalId;

    /// Returns the catalog item's OID.
    fn oid(&self) -> u32;

    /// Returns a description of the result set produced by the catalog item.
    ///
    /// If the catalog item is not of a type that produces data (i.e., an index),
    /// it returns an error.
    /// todo: why FullObjectName?
    fn desc(&self, name: &FullObjectName) -> Result<Cow<RelationDesc>>;

    /// Returns the type of the catalog item.
    fn item_type(&self) -> CatalogItemType;

    /// A normalized SQL statement that describes how to creat the catalog item.
    fn create_sql(&self) -> &str;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CatalogItemType {
    Table,
    Index,
}
