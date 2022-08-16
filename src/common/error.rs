use crate::common::schema::Schema;
use sqlparser::parser::ParserError;
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
    Plan(String),
    SchemaError(SchemaError),
    ParserError(ParserError),
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

/// Create a "table not found" Floppy::SchemaError
pub fn table_not_found(table_name: &str) -> FloppyError {
    FloppyError::SchemaError(SchemaError::TableNotFound(
        format!("table not found: {}", table_name),
    ))
}

// impl From<Error> for FloppyError {
//     fn from(_: Error) -> Self {
//         todo!()
//     }
// }

impl From<ParserError> for FloppyError {
    fn from(e: ParserError) -> Self {
        FloppyError::ParserError(e)
    }
}
