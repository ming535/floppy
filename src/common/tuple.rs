use crate::common::error::Result;
use crate::common::schema::SchemaRef;
use crate::common::value::Value;

/// A `Tuple` represents a tuple in memory.
/// It has contains schema and data.
#[derive(Clone)]
pub struct Tuple {
    schema: SchemaRef,
    values: Vec<Value>,
}

impl Tuple {
    pub fn new(schema: SchemaRef, values: Vec<Value>) -> Self {
        Self { schema, values }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
