mod empty;
mod filter;
mod planner;
mod projection;
mod scan;

use crate::physical_plan::projection::ProjectionExec;
use common::error::{FloppyError, Result};
use common::relation::Row;
use empty::EmptyExec;
use filter::FilterExec;

#[derive(Debug)]
pub enum PhysicalPlan {
    Empty(EmptyExec),
    TableScan,
    IndexScan,
    Filter(FilterExec),
    Projection(ProjectionExec),
}

impl PhysicalPlan {
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
