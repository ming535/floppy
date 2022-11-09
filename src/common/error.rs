use crate::common::relation::{GlobalId, RelationDesc};
use rust_decimal;
use sqlparser::parser::ParserError;
use std::convert::Infallible;
use std::fmt::Formatter;
use std::{fmt, result};

/// Result type for operations that could result in
/// [FloppyError]
pub type Result<T> = result::Result<T, FloppyError>;

/// Error type for generic operations that could result in
/// FloppyError::External
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub enum FloppyError {
    NotImplemented(String),
    /// Error returned as a consequence of an error Floppy.
    /// This error should not happen in normal usage.
    /// Floppy has internal invariant that we are unable to
    /// ask the compiler to check for us. This error is
    /// raised when one of those invariants
    /// is not verified during execution.
    Internal(String),
    Parser(ParserError),
    Plan(String),
    Catalog(CatalogError),
    /// Expression evaluation error
    EvalExpr(String),
    Storage(String),
    Io(std::io::Error),
    ExecuteReturnedResults,
    /// Errors originating from outside Floppy's codebase.
    External(String),
}

#[derive(Debug)]
pub enum CatalogError {
    TableNotFound(String),
    /// No field with this name
    ColumnNotFound {
        qualifier: Option<String>,
        name: String,
        valid_fields: Option<Vec<String>>,
    },
}

/// Create a "field not found" Floppy::SchemaError
pub fn field_not_found(
    qualifier: Option<String>,
    name: &str,
    rel_desc: &RelationDesc,
) -> FloppyError {
    FloppyError::Catalog(CatalogError::ColumnNotFound {
        qualifier,
        name: name.to_string(),
        valid_fields: Some(rel_desc.column_names().clone()),
    })
}

/// Create a "table not found" Floppy::SchemaError
pub fn table_not_found_in_catalog(table_name: &str) -> FloppyError {
    FloppyError::Catalog(CatalogError::TableNotFound(format!(
        "table not found in catalog: {}",
        table_name
    )))
}

pub fn table_not_found_in_storage(table_id: GlobalId) -> FloppyError {
    FloppyError::Storage(format!("table not found in storage: {}", table_id))
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TableNotFound(desc) => {
                write!(f, "{}", desc)
            }
            Self::ColumnNotFound {
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
                            .map(|name| format!("'{}'", name))
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
        FloppyError::Parser(e)
    }
}

impl From<std::io::Error> for FloppyError {
    fn from(e: std::io::Error) -> Self {
        FloppyError::Io(e)
    }
}

impl From<GenericError> for FloppyError {
    fn from(e: GenericError) -> Self {
        FloppyError::External(e.to_string())
    }
}

impl From<rust_decimal::Error> for FloppyError {
    fn from(e: rust_decimal::Error) -> Self {
        FloppyError::External(e.to_string())
    }
}

impl fmt::Display for FloppyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FloppyError::NotImplemented(desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
            FloppyError::Internal(desc) => {
                write!(
                    f,
                    "Internal error: {}. This was likely caused by a bug",
                    desc
                )
            }
            FloppyError::Plan(desc) => {
                write!(f, "Planner error: {}", desc)
            }
            FloppyError::EvalExpr(desc) => {
                write!(f, "Expression evaluation error: {}", desc)
            }
            FloppyError::Storage(desc) => write!(f, "Storage error: {}", desc),
            FloppyError::Catalog(e) => {
                write!(f, "Schema error: {}", e)
            }
            FloppyError::Parser(e) => {
                write!(f, "Parser error: {}", e)
            }
            FloppyError::Io(e) => {
                write!(f, "Io error: {}", e)
            }
            FloppyError::ExecuteReturnedResults => {
                write!(f, "Execute returned results")
            }
            FloppyError::External(e) => {
                write!(f, "external error: {}", e)
            }
        }
    }
}
