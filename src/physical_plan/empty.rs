use crate::common::error::Result;
use crate::common::row::Row;
use crate::common::schema::{
    DataType, Field, Schema, SchemaRef,
};
use crate::common::value::Value;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

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
