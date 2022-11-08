use crate::error::{field_not_found, FloppyError, Result};
use crate::scalar::{Datum, ScalarType};
use std::cmp::Ordering;
use std::ops::{Bound, Index, RangeBounds};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnType {
    pub scalar_type: ScalarType,
    pub nullable: bool,
}

impl ColumnType {
    pub fn new(scalar_type: ScalarType, nullable: bool) -> Self {
        Self {
            scalar_type,
            nullable,
        }
    }
}

/// The type of a relation.
#[derive(Debug, Clone)]
pub struct RelationType {
    /// The type for each column, in order.
    column_types: Vec<ColumnType>,
    /// Indices that represents a primary key.
    /// If the user haven't specify a primary key, then a
    /// `RowId` is generated for this table as a primary
    /// key. A table can have only one primary index.
    prim_key: Vec<usize>,
    /// Sets of indices that are "secondary keys" for this
    /// relation. A relation can have multiple secondary
    /// indices.
    secondary_keys: Vec<Vec<usize>>,
}

impl RelationType {
    pub fn new(
        column_types: Vec<ColumnType>,
        prim_key: Vec<usize>,
        secondary_keys: Vec<Vec<usize>>,
    ) -> Self {
        Self {
            column_types,
            prim_key,
            secondary_keys,
        }
    }

    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self {
            column_types: vec![],
            prim_key: vec![],
            secondary_keys: vec![],
        }
    }

    /// Get a list of fields
    pub fn column_types(&self) -> &Vec<ColumnType> {
        &self.column_types
    }

    /// Returns an immutable reference of a specified
    /// `Field` instance selected using an offset within
    /// the internal `fields` vector
    pub fn column_type(&self, i: usize) -> &ColumnType {
        &self.column_types[i]
    }
}

pub type ColumnName = String;

/// A description of the shape of a relation.
///
/// It bundles a [`RelationType`] with the name of each
/// column in the relation.
///
/// To simplify the design, we assume that column is never
/// deleted in a table, so that a column's index in the
/// Vector uniquely identify a valid column.
#[derive(Debug, Clone)]
pub struct RelationDesc {
    rel_type: RelationType,
    column_names: Vec<ColumnName>,
}

impl Default for RelationDesc {
    fn default() -> Self {
        Self::empty()
    }
}

impl RelationDesc {
    pub fn new(
        column_types: Vec<ColumnType>,
        column_names: Vec<String>,
        prim_key: Vec<usize>,
        secondary_keys: Vec<Vec<usize>>,
    ) -> Self {
        Self {
            rel_type: RelationType::new(column_types, prim_key, secondary_keys),
            column_names,
        }
    }

    pub fn empty() -> Self {
        Self {
            rel_type: RelationType::empty(),
            column_names: vec![],
        }
    }

    pub fn rel_type(&self) -> &RelationType {
        &self.rel_type
    }

    pub fn column_types(&self) -> &Vec<ColumnType> {
        self.rel_type.column_types()
    }

    pub fn column_type(&self, idx: usize) -> Result<&ColumnType> {
        let typs = self.rel_type.column_types();
        if idx >= typs.len() {
            Err(FloppyError::Internal(format!(
                "column index out of range, idx = {}, length = {}",
                idx,
                typs.len()
            )))
        } else {
            Ok(&typs[idx])
        }
    }

    pub fn column_names(&self) -> &Vec<String> {
        &self.column_names
    }

    pub fn column_idx(&self, column_name: &str) -> Result<usize> {
        let mut matches = self
            .column_names
            .iter()
            .enumerate()
            .filter(|(_, name)| column_name == *name)
            .map(|(idx, _)| idx);
        match matches.next() {
            None => Err(field_not_found(None, column_name, self)),
            Some(idx) => match matches.next() {
                None => Ok(idx),
                Some(_) => Err(FloppyError::Internal(format!(
                    "duplicated column name: {}",
                    column_name
                ))),
            },
        }
    }

    pub fn column_name(&self, idx: usize) -> &str {
        self.column_names[idx].as_str()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ColumnName, &ColumnType)> {
        self.iter_names().zip(self.iter_types())
    }

    pub fn iter_types(&self) -> impl Iterator<Item = &ColumnType> {
        self.rel_type.column_types.iter()
    }

    pub fn iter_names(&self) -> impl Iterator<Item = &ColumnName> {
        self.column_names.iter()
    }

    pub fn prim_key(&self) -> Vec<usize> {
        self.rel_type.prim_key.clone()
    }
}

/// Describe the output of a SQL statement.
#[derive(Debug, Clone)]
pub struct StatementDesc {
    /// The shape of the rows produced by the statement, if
    /// the statement produces rows.
    pub rel_desc: Option<RelationDesc>,
    /// The determined types of the parameters in the
    /// statement, if any.
    pub param_types: Vec<ScalarType>,
}

impl StatementDesc {
    /// Reports the number of columns in the statement's
    /// result set, or zero if the statement does not
    /// return rows.
    pub fn arity(&self) -> usize {
        self.rel_desc
            .as_ref()
            .map(|desc| desc.column_types().len())
            .unwrap_or(0)
    }
}

/// A vector of values to which parameter references should
/// be bound.
#[derive(Debug, Clone)]
pub struct Params {
    pub datums: Row,
    pub types: Vec<ScalarType>,
}

impl Params {
    pub fn empty() -> Self {
        Self {
            datums: Row::empty(),
            types: vec![],
        }
    }
}

/// A `Row` represents a tuple in memory.
/// It has contains schema and data.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    values: Vec<Datum>,
}

impl Row {
    pub fn new(values: Vec<Datum>) -> Self {
        Self { values }
    }

    pub fn empty() -> Self {
        Row::new(vec![])
    }

    pub fn column_value(&self, index: usize) -> Result<Datum> {
        if index > self.values.len() {
            return Err(FloppyError::Internal(format!(
                "column index out of range, column index = {:}, column len = {:}",
                index,
                self.values.len()
            )));
        }
        Ok(self.values[index].clone())
    }

    pub fn prim_key_datums(&self, rel_desc: &RelationDesc) -> Result<IndexKeyDatums> {
        let prim_key = rel_desc.prim_key();
        prim_key
            .iter()
            .map(|i| {
                if *i >= self.values.len() {
                    Err(FloppyError::Internal(format!(
                        "primary key index out of range: {:?}",
                        rel_desc
                    )))
                } else {
                    let datum = self.values[*i].clone();
                    Ok(datum)
                }
            })
            .collect::<Result<IndexKeyDatums>>()
    }
}

/// A column reference in a [`Row`], used by expressions.
#[derive(Debug, Clone)]
pub struct ColumnRef {
    /// column identifier
    pub id: usize,
    pub name: ColumnName,
}

/// Unique id in the system.
/// Every table, index, database, schema has a unique id.
pub type GlobalId = u64;

/// IndexKey is a sorted column datums.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct IndexKeyDatums(Vec<Datum>);

impl FromIterator<Datum> for IndexKeyDatums {
    fn from_iter<T: IntoIterator<Item = Datum>>(iter: T) -> Self {
        let datums: Vec<Datum> = Vec::from_iter(iter);
        Self(datums)
    }
}

// impl PartialOrd<Self> for IndexKeyDatums {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         todo!()
//     }
// }
//
// impl Ord for IndexKeyDatums {
//     fn cmp(&self, other: &Self) -> Ordering {
//         assert_eq!(self.0.len(), other.0.len());
//         self.partial_cmp()
//         todo!()
//     }
// }

/// IndexRange represent the index's boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRange {
    pub lo: Bound<IndexKeyDatums>,
    pub hi: Bound<IndexKeyDatums>,
}

impl RangeBounds<IndexKeyDatums> for IndexRange {
    fn start_bound(&self) -> Bound<&IndexKeyDatums> {
        match &self.lo {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(b) => Bound::Included(b),
            Bound::Excluded(b) => Bound::Excluded(b),
        }
    }

    fn end_bound(&self) -> Bound<&IndexKeyDatums> {
        match &self.hi {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(b) => Bound::Included(b),
            Bound::Excluded(b) => Bound::Excluded(b),
        }
    }
}

mod tests {
    use super::*;
    use std::ops::Range;
    #[test]
    fn key_range() {
        let key_start = IndexKeyDatums(vec![Datum::Int64(1), Datum::Int64(2)]);
        let key_end = IndexKeyDatums(vec![Datum::Int64(3), Datum::Int64(4)]);
        assert_eq!(
            (key_start.clone()..key_end.clone()),
            Range {
                start: key_start.clone(),
                end: key_end.clone()
            }
        );

        assert_eq!(key_start < key_end, true);
    }
}
