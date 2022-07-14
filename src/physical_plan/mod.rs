use crate::common::error::Result;
use crate::common::schema::SchemaRef;
use crate::common::tuple::Tuple;
use futures::Stream;
use std::pin::Pin;

pub trait TupleStream:
    Stream<Item = Result<Tuple>>
{
    fn schema(&self) -> SchemaRef;
}

pub type SendableTupleStream =
    Pin<Box<dyn TupleStream + Send>>;

mod display;
mod empty;
mod plan;
mod planner;
mod projection;
