use crate::common::error::Result;
use crate::common::schema::SchemaRef;
use crate::common::tuple::Tuple;
use futures::Stream;

pub trait TupleStream: Stream<Item = Result<Tuple>> {
    fn schema(&self) -> SchemaRef;
}

pub type SendableTupleStream = Box<dyn TupleStream + Send>;

mod empty;
mod plan;
mod planner;
mod projection;
