use crate::common::error::{FloppyError, Result};
use crate::common::schema::SchemaRef;
use crate::common::value::Value;

/// A `Tuple` represents a tuple in memory.
/// It has contains schema and data.
#[derive(Debug, Clone)]
pub struct Tuple {
    values: Vec<Value>,
}

pub type BlockId = i64;

pub type ItemId = i64;

pub struct TupleId {
    pub block_id: BlockId,
    pub item_id: ItemId,
}

impl Tuple {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn value(&self, index: usize) -> Result<Value> {
        if index > self.values.len() {
            return Err(FloppyError::Internal(format!(
                "column index out of range, column index = {:}, column len = {:}",
                index,
                self.values.len()
            )));
        }
        Ok(self.values[index].clone())
    }
}
