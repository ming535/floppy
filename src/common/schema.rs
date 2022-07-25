use crate::common::error::{
    field_not_found, FloppyError, Result,
};
use crate::logical_expr::column::Column;
use std::sync::Arc;

/// DataType defines data type used in schema.
/// Data type defined in SQL is translated into
/// this internal `DataType`.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    /// A variable-length string in Unicode with UTF-8 encoding.
    Utf8,
}

impl DataType {
    pub fn is_signed_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
        )
    }

    pub fn is_unsigned_numeric(&self) -> bool {
        matches!(
            self,
            DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    }

    pub fn is_numeric(&self) -> bool {
        self.is_signed_numeric()
            || self.is_unsigned_numeric()
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    /// Optional qualifier (usually a table or relation name)
    qualifier: Option<String>,
    /// Field's name
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl Field {
    pub fn new(
        qualifier: Option<&str>,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Field {
            qualifier: qualifier.map(|s| s.to_owned()),
            name: name.to_string(),
            data_type,
            nullable,
        }
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Builds a qualified column based on self
    pub fn qualified_column(&self) -> Column {
        Column {
            relation: self.qualifier.clone(),
            name: self.name.clone(),
        }
    }

    pub fn qualified_name(&self) -> String {
        if let Some(qualifier) = &self.qualifier {
            format!("{}.{}", qualifier, self.name)
        } else {
            self.name.clone()
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self { fields: vec![] }
    }

    /// Get a list of fields
    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    /// Returns an immutable reference of a specified `Field` instance
    /// selected using an offset within the internal `fields` vector
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    /// Get list of fully-qualified names in this schema
    pub fn field_names(&self) -> Vec<String> {
        self.fields
            .iter()
            .map(|f| f.qualified_name())
            .collect::<Vec<_>>()
    }

    /// Find the field with the given name
    pub fn field_with_unqualified_name(
        &self,
        name: &str,
    ) -> Result<&Field> {
        let fields =
            self.fields_with_unqualified_name(name);
        match fields.len() {
            0 => Err(field_not_found(None, name, self)),
            1 => Ok(fields[0]),
            _ => Err(field_not_found(None, name, self)),
        }
    }

    /// Find the field with the given qualified name
    pub fn field_with_qualified_name(
        &self,
        qualifier: &str,
        name: &str,
    ) -> Result<&Field> {
        let idx = self.index_of_column_by_name(
            Some(qualifier),
            name,
        )?;
        Ok(self.field(idx))
    }

    /// Find all fields match the give name
    pub fn fields_with_unqualified_name(
        &self,
        name: &str,
    ) -> Vec<&Field> {
        self.fields
            .iter()
            .filter(|f| f.name() == name)
            .collect()
    }

    pub fn index_of_column(
        &self,
        col: &Column,
    ) -> Result<usize> {
        self.index_of_column_by_name(
            col.relation.as_deref(),
            &col.name,
        )
    }

    pub fn index_of_column_by_name(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<usize> {
        let mut matches = self
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                match (qualifier, &field.qualifier) {
                    // field to lookup is qualified.
                    // current field is qualified and not shared between relations, compare
                    // both qualifier and name.
                    (Some(q), Some(field_q)) => {
                        q == field_q && field.name == name
                    }
                    // field to lookup is qualified but current field is unqualified.
                    (Some(_), None) => false,
                    // field to lookup is unqualified, no need to compare qualifier
                    (None, Some(_)) | (None, None) => {
                        field.name == name
                    }
                }
            })
            .map(|(idx, _)| idx);
        match matches.next() {
            None => Err(field_not_found(
                qualifier.map(|s| s.to_string()),
                name,
                self,
            )),
            Some(idx) => match matches.next() {
                None => Ok(idx),
                Some(_) => Err(FloppyError::Internal(format!(
                    "Ambiguous reference to qualified field name '{}.{}'",
                    qualifier.unwrap_or("<unqualified>"),
                    name
                ))),
            },
        }
    }
}

pub type SchemaRef = Arc<Schema>;
