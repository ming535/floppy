use crate::common::schema::Schema;
use std::fmt::Error;
use std::result;

pub type Result<T> = result::Result<T, FloppyError>;

#[derive(Debug)]
pub enum FloppyError {
    NotImplemented(String),
    /// Error returned as a consequence of an error Floppy.
    /// This error should not happen in normal usage.
    /// Floppy has internal invariant that we are unable to ask the compiler
    /// to check for us. This error is raised when one of those invariants
    /// is not verified during execution.
    Internal(String),
    ParseError(String),
    Plan(String),
    SchemaError(SchemaError),
}

#[derive(Debug)]
pub enum SchemaError {
    TableNotFound(String),
    /// No field with this name
    FieldNotFound {
        qualifier: Option<String>,
        name: String,
        valid_fields: Option<Vec<String>>,
    },
}

/// Create a "field not found" Floppy::SchemaError
pub fn field_not_found(
    qualifier: Option<String>,
    name: &str,
    schema: &Schema,
) -> FloppyError {
    FloppyError::SchemaError(SchemaError::FieldNotFound {
        qualifier,
        name: name.to_string(),
        valid_fields: Some(schema.field_names()),
    })
}

impl From<std::fmt::Error> for FloppyError {
    fn from(_: Error) -> Self {
        todo!()
    }
}
