use common::error::Result;
use common::row::Row;

use common::value::Value;

#[derive(Clone)]
pub struct EmptyExec {
    index: usize,
}

impl EmptyExec {
    pub fn new() -> Self {
        Self { index: 0 }
    }
}

impl EmptyExec {
    pub fn next(&mut self) -> Result<Option<Row>> {
        if self.index == 1 {
            return Ok(None);
        }

        self.index += 1;
        let values = vec![Value::Null];
        let row = Row::new(values);
        Ok(Some(row))
    }
}
