use crate::common::field::Field;
use std::sync::Arc;

pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self { fields: vec![] }
    }

    /// Get a list of fields
    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }
}

pub type SchemaRef = Arc<Schema>;
