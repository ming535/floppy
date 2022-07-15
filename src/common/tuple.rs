use crate::common::error::{FloppyError, Result};
use crate::common::schema::SchemaRef;
use crate::common::value::Value;

/// A `Tuple` represents a tuple in memory.
/// It has contains schema and data.
#[derive(Debug, Clone)]
pub struct Tuple {
    schema: SchemaRef,
    values: Vec<Value>,
}

impl Tuple {
    pub fn new(
        schema: SchemaRef,
        values: Vec<Value>,
    ) -> Self {
        Self { schema, values }
    }

    pub fn value(&self, index: usize) -> Result<Value> {
        if index > self.values.len() {
            return Err(FloppyError::Internal(format!(
                "column index out of range, index = {:}, len = {:}",
                index,
                self.values.len()
            )));
        }
        Ok(self.values[index].clone())
    }
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
