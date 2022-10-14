mod empty;
mod filter;
mod projection;
mod scan;

use crate::physical_plan::projection::ProjectionExec;
use common::error::{FloppyError, Result};
use common::relation::Row;
use empty::EmptyExec;
use filter::FilterExec;

#[derive(Debug)]
pub enum PhysicalPlan<'a, 'b> {
    Empty(EmptyExec),
    TableScan,
    IndexScan,
    Filter(FilterExec<'a, 'b>),
    Projection(ProjectionExec<'a, 'b>),
}

impl<'a, 'b> PhysicalPlan<'a, 'b> {
    pub fn next(&mut self) -> Result<Option<Row>> {
        match self {
            Self::Empty(p) => p.next(),
            Self::Filter(p) => p.next(),
            Self::Projection(p) => p.next(),
            _ => Err(FloppyError::NotImplemented(format!(
                "physical plan not implemented: {:?}",
                self
            ))),
        }
    }
}
