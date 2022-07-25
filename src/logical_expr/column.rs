use crate::common::error::FloppyError;
use crate::common::error::Result;
use crate::common::schema::Schema;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone)]
pub struct Column {
    /// relation/table name.
    pub relation: Option<String>,
    /// field/column name.
    pub name: String,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.relation {
            Some(ref r) => {
                write!(f, "#{}.{}", r, self.name)
            }
            None => write!(f, "#{}", self.name),
        }
    }
}

impl Column {
    pub fn normalize_with_schema(
        self,
        schema: &Schema,
    ) -> Result<Self> {
        if self.relation.is_some() {
            return Ok(self);
        }

        let fields =
            schema.fields_with_unqualified_name(&self.name);
        match fields.len() {
            1 => Ok(fields[0].qualified_column()),
            _ => Err(FloppyError::Internal(
                "failed to normalize column".to_string(),
            )),
        }
    }

    pub fn normalize_with_schemas(
        self,
        schemas: &[&Arc<Schema>],
    ) -> Result<Self> {
        if self.relation.is_some() {
            return Ok(self);
        }

        for schema in schemas {
            let fields = schema
                .fields_with_unqualified_name(&self.name);
            match fields.len() {
                0 => continue,
                1 => {
                    return Ok(fields[0].qualified_column());
                }
                _ => {
                    return Err(FloppyError::Internal(
                        "failed to normalize column"
                            .to_string(),
                    ));
                }
            }
        }

        Err(FloppyError::Internal(
            "failed to normalize column".to_string(),
        ))
    }
}
