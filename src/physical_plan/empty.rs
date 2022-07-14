use crate::common::error::Result;
use crate::common::schema::{
    DataType, Field, Schema, SchemaRef,
};
use crate::common::tuple::Tuple;
use crate::common::value::Value;
use crate::physical_plan::{
    SendableTupleStream, TupleStream,
};
use futures::Stream;
use std::fmt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct EmptyExec {
    pub schema: SchemaRef,
}

impl EmptyExec {
    pub fn execute(&self) -> Result<SendableTupleStream> {
        let schema =
            Arc::new(Schema::new(vec![Field::new(
                None,
                "placeholder",
                DataType::Null,
                true,
            )]));
        let values = vec![Value::Null];
        let tuple = Tuple::new(schema.clone(), values);
        Ok(Box::pin(EmptyStream::new(
            schema.clone(),
            vec![tuple],
        )))
    }
}

pub struct EmptyStream {
    schema: SchemaRef,
    data: Vec<Tuple>,
    index: usize,
}

impl EmptyStream {
    fn new(schema: SchemaRef, data: Vec<Tuple>) -> Self {
        EmptyStream {
            schema,
            data,
            index: 0,
        }
    }
}

impl Stream for EmptyStream {
    type Item = Result<Tuple>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let item = if self.index < self.data.len() {
            self.index += 1;
            let tuple = &self.data[self.index - 1];
            Some(Ok(tuple.clone()))
        } else {
            None
        };
        Poll::Ready(item)
    }
}

impl TupleStream for EmptyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
