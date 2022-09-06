use crate::schema::Schema;
use sqlparser::parser::ParserError;
use std::fmt::Formatter;
use std::{fmt, result};

/// Result type for operations that could result in [FloppyError]
pub type Result<T> = result::Result<T, FloppyError>;

/// Error type for generic operations that could result in FloppyError::External
pub type GenericError =
    Box<dyn std::error::Error + Send + Sync>;

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
    IoError(std::io::Error),
    /// Errors originating from outside Floppy's codebase.
    External(GenericError),
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

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TableNotFound(desc) => {
                write!(f, "{}", desc)
            }
            Self::FieldNotFound {
                qualifier,
                name,
                valid_fields,
            } => {
                write!(f, "No field named ")?;
                if let Some(q) = qualifier {
                    write!(f, "'{}.{}'", q, name)?;
                } else {
                    write!(f, "'{}'", name)?;
                }
                if let Some(field_names) = valid_fields {
                    write!(
                        f,
                        ". Valid fields are {}",
                        field_names
                            .iter()
                            .map(|name| format!(
                                "'{}'",
                                name
                            ))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )?;
                }
                write!(f, ".")
            }
        }
    }
}

impl From<ParserError> for FloppyError {
    fn from(e: ParserError) -> Self {
        FloppyError::ParserError(e)
    }
}

impl From<std::io::Error> for FloppyError {
    fn from(e: std::io::Error) -> Self {
        FloppyError::IoError(e)
    }
}

impl From<GenericError> for FloppyError {
    fn from(e: GenericError) -> Self {
        FloppyError::External(e)
    }
}

impl fmt::Display for FloppyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FloppyError::NotImplemented(desc) => {
                write!(
                    f,
                    "This feature is not implemented: {}",
                    desc
                )
            }
            FloppyError::Internal(desc) => {
                write!(f, "Internal error: {}. This was likely caused by a bug", desc)
            }
            FloppyError::Plan(desc) => {
                write!(f, "Planner error: {}", desc)
            }
            FloppyError::SchemaError(e) => {
                write!(f, "Schema error: {}", e)
            }
            FloppyError::ParserError(e) => {
                write!(f, "Parser error: {}", e)
            }
            FloppyError::IoError(e) => {
                write!(f, "Io error: {}", e)
            }
            FloppyError::External(e) => {
                write!(f, "external error: {}", e)
            }
        }
    }
}
