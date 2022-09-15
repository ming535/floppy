use common::error::Result;
use common::relation::Row;

use common::scalar::Datum;

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
        let values = vec![Datum::Null];
        let row = Row::new(values);
        Ok(Some(row))
    }
}
