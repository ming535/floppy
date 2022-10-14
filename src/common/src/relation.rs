use crate::error::{field_not_found, FloppyError, Result};
use crate::scalar::{Datum, ScalarType};
use std::sync::Arc;

#[derive(Debug, Clone)]
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
    // /// Builds a qualified column based on self
    // pub fn qualified_column(&self) -> Column {
    //     Column {
    //         relation: self.qualifier.clone(),
    //         name: self.name.clone(),
    //     }
    // }
    //
    // pub fn qualified_name(&self) -> String {
    //     if let Some(qualifier) = &self.qualifier {
    //         format!("{}.{}", qualifier, self.name)
    //     } else {
    //         self.name.clone()
    //     }
    // }
    //
    // pub fn name(&self) -> String {
    //     self.name.clone()
    // }
}

//// A named reference to a qualified field in a schema.
// #[derive(Debug, Clone)]
// pub struct Column {
//     /// relation/table name.
//     pub relation: Option<String>,
//     /// field/column name.
//     pub name: String,
// }
//
// impl fmt::Display for Column {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         match self.relation {
//             Some(ref r) => {
//                 write!(f, "#{}.{}", r, self.name)
//             }
//             None => write!(f, "#{}", self.name),
//         }
//     }
// }
//
// impl Column {
//     pub fn normalize_with_schema(
//         self,
//         schema: &Schema,
//     ) -> Result<Self> {
//         if self.relation.is_some() {
//             return Ok(self);
//         }
//
//         let fields =
//             schema.fields_with_unqualified_name(&self.name);
//         match fields.len() {
//             1 => Ok(fields[0].qualified_column()),
//             _ => Err(FloppyError::Internal(
//                 "failed to normalize column".to_string(),
//             )),
//         }
//     }
//
//     pub fn normalize_with_schemas(
//         self,
//         schemas: &[&Arc<Schema>],
//     ) -> Result<Self> {
//         if self.relation.is_some() {
//             return Ok(self);
//         }
//
//         for schema in schemas {
//             let fields = schema
//                 .fields_with_unqualified_name(&self.name);
//             match fields.len() {
//                 0 => continue,
//                 1 => {
//                     return Ok(fields[0].qualified_column());
//                 }
//                 _ => {
//                     return Err(FloppyError::Internal(
//                         "failed to normalize column"
//                             .to_string(),
//                     ));
//                 }
//             }
//         }
//
//         Err(FloppyError::Internal(
//             "failed to normalize column".to_string(),
//         ))
//     }
// }

#[derive(Debug, Clone)]
pub struct RelationType {
    column_types: Vec<ColumnType>,
}

impl RelationType {
    pub fn new(column_types: Vec<ColumnType>) -> Self {
        Self { column_types }
    }

    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self {
            column_types: vec![],
        }
    }

    /// Get a list of fields
    pub fn column_types(&self) -> &Vec<ColumnType> {
        &self.column_types
    }

    /// Returns an immutable reference of a specified `Field` instance
    /// selected using an offset within the internal `fields` vector
    pub fn column_type(&self, i: usize) -> &ColumnType {
        &self.column_types[i]
    }

    //// Get list of fully-qualified names in this schema
    // pub fn field_names(&self) -> Vec<String> {
    //     self.column_types
    //         .iter()
    //         .map(|f| f.qualified_name())
    //         .collect::<Vec<_>>()
    // }

    //// Find the field with the given name
    // pub fn field_with_unqualified_name(
    //     &self,
    //     name: &str,
    // ) -> Result<&ColumnType> {
    //     let fields =
    //         self.fields_with_unqualified_name(name);
    //     match fields.len() {
    //         0 => Err(field_not_found(None, name, self)),
    //         1 => Ok(fields[0]),
    //         _ => Err(field_not_found(None, name, self)),
    //     }
    // }

    //// Find the field with the given qualified name
    // pub fn field_with_qualified_name(
    //     &self,
    //     qualifier: &str,
    //     name: &str,
    // ) -> Result<&ColumnType> {
    //     let idx = self.index_of_column_by_name(
    //         Some(qualifier),
    //         name,
    //     )?;
    //     Ok(self.field(idx))
    // }

    ////    Find all fields match the give name
    // pub fn fields_with_unqualified_name(
    //     &self,
    //     name: &str,
    // ) -> Vec<&ColumnType> {
    //     self.column_types
    //         .iter()
    //         .filter(|f| f.name() == name)
    //         .collect()
    // }

    // pub fn index_of_column(
    //     &self,
    //     col: &Column,
    // ) -> Result<usize> {
    //     self.index_of_column_by_name(
    //         col.relation.as_deref(),
    //         &col.name,
    //     )
    // }

    // pub fn index_of_column_by_name(
    //     &self,
    //     qualifier: Option<&str>,
    //     name: &str,
    // ) -> Result<usize> {
    //     let mut matches = self
    //         .column_types
    //         .iter()
    //         .enumerate()
    //         .filter(|(_, field)| {
    //             match (qualifier, &field.qualifier) {
    //                 // field to lookup is qualified.
    //                 // current field is qualified and not shared between relations, compare
    //                 // both qualifier and name.
    //                 (Some(q), Some(field_q)) => {
    //                     q == field_q && field.name == name
    //                 }
    //                 // field to lookup is qualified but current field is unqualified.
    //                 (Some(_), None) => false,
    //                 // field to lookup is unqualified, no need to compare qualifier
    //                 (None, Some(_)) | (None, None) => {
    //                     field.name == name
    //                 }
    //             }
    //         })
    //         .map(|(idx, _)| idx);
    //     match matches.next() {
    //         None => Err(field_not_found(
    //             qualifier.map(|s| s.to_string()),
    //             name,
    //             self,
    //         )),
    //         Some(idx) => match matches.next() {
    //             None => Ok(idx),
    //             Some(_) => Err(FloppyError::Internal(format!(
    //                 "Ambiguous reference to qualified field name '{}.{}'",
    //                 qualifier.unwrap_or("<unqualified>"),
    //                 name
    //             ))),
    //         },
    //     }
    // }
}

pub type ColumnName = String;

/// A description of the shape of a relation.
///
/// It bundles a [`RelationType`] with the name of each column in the raltion.
///
#[derive(Debug, Clone)]
pub struct RelationDesc {
    rel_type: RelationType,
    column_names: Vec<ColumnName>,
}

impl RelationDesc {
    pub fn new(column_types: Vec<ColumnType>, column_names: Vec<String>) -> Self {
        Self {
            rel_type: RelationType::new(column_types),
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
}

pub type RelationDescRef = Arc<RelationDesc>;

/// Describe the output of a SQL statement.
#[derive(Debug, Clone)]
pub struct StatementDesc {
    /// The shape of the rows produced by the statement, if the statement
    /// produces rows.
    pub rel_desc: Option<RelationDesc>,
    /// The determined types of the parameters in the statement, if any.
    pub param_types: Vec<ScalarType>,
}

impl StatementDesc {
    /// Reports the number of columns in the statement's result set, or zero
    /// if the statement does not return rows.
    pub fn arity(&self) -> usize {
        self.rel_desc
            .as_ref()
            .map(|desc| desc.column_types().len())
            .unwrap_or(0)
    }
}

/// A vector of values to which parameter references should be bound.
#[derive(Debug, Clone)]
pub struct Params {
    pub datums: Row,
    pub types: Vec<ScalarType>,
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

// #[derive(Debug, Clone, Serialize)]
// pub struct Table {
//     pub create_sql: String,
//     pub desc: RelationDesc,
// }
//
// pub struct Index {
//     pub create_sql: String,
//     pub on: GlobalId,
//     pub keys: Vec<ScalarExpr>,
// }
