use crate::{FLOPPY_DB_ID, FLOPPY_DB_NAME, FLOPPY_SCHEMA_ID, FLOPPY_SCHEMA_NAME};
use common::error::FloppyError;
use serde::{Deserialize, Serialize};
use sqlparser::ast::ObjectName as SqlObjectName;

/// A fully-qualified human readable name of an item in the catalog.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FullObjectName {
    pub database: String,
    pub schema: String,
    pub item: String,
}

impl From<PartialObjectName> for FullObjectName {
    fn from(partial_name: PartialObjectName) -> Self {
        let database = partial_name
            .database
            .map_or(FLOPPY_DB_NAME.to_string(), |v| v);
        let schema = partial_name
            .schema
            .map_or(FLOPPY_SCHEMA_NAME.to_string(), |v| v);
        Self {
            database,
            schema,
            item: partial_name.item,
        }
    }
}

impl From<String> for FullObjectName {
    fn from(s: String) -> Self {
        Self {
            database: FLOPPY_DB_NAME.to_string(),
            schema: FLOPPY_SCHEMA_NAME.to_string(),
            item: s,
        }
    }
}

impl From<&str> for FullObjectName {
    fn from(s: &str) -> Self {
        Self {
            database: FLOPPY_DB_NAME.to_string(),
            schema: FLOPPY_SCHEMA_NAME.to_string(),
            item: s.to_string(),
        }
    }
}

/// An optionally-qualified human-readable name of an item in the catalog.
///
/// This is like a [`FullObjectName`], but either the database or schema name may be
/// omitted.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct PartialObjectName {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub item: String,
}

impl TryFrom<&SqlObjectName> for PartialObjectName {
    type Error = FloppyError;

    fn try_from(value: &SqlObjectName) -> Result<Self, Self::Error> {
        let len = value.0.len();
        if len > 3 || len == 0 {
            return Err(FloppyError::Plan(format!(
                "objet name length invalid: object name = {}, length = {}",
                value,
                value.0.len()
            )));
        }

        let item = value.0[len - 1].value.clone();
        let schema = if len >= 2 {
            Some(value.0[len - 2].value.clone())
        } else {
            None
        };
        let database = if len >= 3 {
            Some(value.0[len - 3].value.clone())
        } else {
            None
        };

        Ok(PartialObjectName {
            database,
            schema,
            item,
        })
    }
}

/// A fully-qualified non-human readable name of an item in the catalog using IDs for the database
/// and schema.
#[derive(Debug)]
pub struct QualifiedObjectName {
    pub qualifiers: ObjectQualifiers,
    pub item: String,
}

impl From<&str> for QualifiedObjectName {
    fn from(s: &str) -> Self {
        Self {
            qualifiers: ObjectQualifiers {
                database: FLOPPY_DB_ID,
                schema: FLOPPY_SCHEMA_ID,
            },
            item: s.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct ObjectQualifiers {
    pub database: DatabaseId,
    pub schema: SchemaId,
}

/// The identifier for a database
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct DatabaseId(pub u64);

/// The identifier for a schema.
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct SchemaId(pub u64);

#[cfg(test)]
mod tests {
    use super::*;
    use common::error::Result;
    use sqlparser::ast::Ident;

    #[test]
    fn test_sql_object_to_partial_object_name() -> Result<()> {
        let object_name = &SqlObjectName(vec![
            Ident {
                value: "test".to_string(),
                quote_style: None,
            },
            Ident {
                value: "public".to_string(),
                quote_style: None,
            },
            Ident {
                value: "test_table".to_string(),
                quote_style: None,
            },
        ]);

        let partial_name: PartialObjectName = object_name.try_into()?;
        assert_eq!(partial_name.database, Some("test".to_string()));
        assert_eq!(partial_name.schema, Some("public".to_string()));
        assert_eq!(partial_name.item, "test_table".to_string());
        Ok(())
    }
}
