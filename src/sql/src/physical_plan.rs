mod empty;
mod filter;
pub mod planner;
mod pri_scan;
mod projection;
mod sec_scan;

use crate::context::ExecutionContext;
use crate::physical_plan::pri_scan::PriKeyScanExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::sec_scan::SecKeyScan;
use common::error::{FloppyError, Result};
use common::relation::Row;
use empty::EmptyExec;
use filter::FilterExec;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug)]
pub enum PhysicalPlan {
    Empty(EmptyExec),
    /// Scan the table with primary index range.
    PriKeyScan(PriKeyScanExec),
    /// Scan the table using secondary index range.
    SecKeyScan(SecKeyScan),
    Filter(FilterExec),
    Projection(ProjectionExec),
}

impl PhysicalPlan {
    /// `stream` compile/returns a graph of `Stream` that is
    /// ready to be executed.
    pub fn stream(&self, exec_ctx: Arc<ExecutionContext>) -> Result<RowStream> {
        match self {
            Self::Empty(p) => p.stream(exec_ctx.clone()),
            Self::Filter(p) => p.stream(exec_ctx.clone()),
            Self::Projection(p) => p.stream(exec_ctx.clone()),
            Self::PriKeyScan(p) => p.stream(exec_ctx),
            _ => Err(FloppyError::NotImplemented(format!(
                "physical sql not implemented: {:?}",
                self
            ))),
        }
    }
}

/// Trait for iterator execution.
/// The actual logic of different physical relational
/// operators is implemented in various `RowStream`
pub type RowStream = Pin<Box<dyn Stream<Item = Result<Row>>>>;
