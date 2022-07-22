use crate::common::error::Result;
use crate::common::row::Row;
use crate::common::schema::SchemaRef;
use futures::Stream;
use std::pin::Pin;

pub trait TupleStream: Stream<Item = Result<Row>> {
    fn schema(&self) -> SchemaRef;
}

pub type SendableTupleStream =
    Pin<Box<dyn TupleStream + Send>>;

mod display;
mod empty;
mod heap_scan;
mod plan;
mod planner;
mod projection;
