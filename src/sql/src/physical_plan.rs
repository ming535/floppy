mod empty;
mod filter;
mod index_scan;
pub mod planner;
mod projection;
mod table_scan;

use crate::physical_plan::index_scan::SecondaryIndexScan;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::table_scan::{FullTableScanExec, PrimaryIndexTableScanExec};
use common::error::{FloppyError, Result};
use common::relation::Row;
use empty::EmptyExec;
use filter::FilterExec;
use futures::Stream;
use std::pin::Pin;

#[derive(Debug)]
pub enum PhysicalPlan {
    Empty(EmptyExec),
    /// Scan from the full table.
    FullScan(FullTableScanExec),
    /// Scan the table with primary index range.
    PrimaryIndexScan(PrimaryIndexTableScanExec),
    /// Scan the table using secondary index range.
    SecondaryIndexScan(SecondaryIndexScan),
    Filter(FilterExec),
    Projection(ProjectionExec),
}

impl PhysicalPlan {
    /// `stream` compile/returns a graph of `Stream` that is ready to be executed.
    pub fn stream(&self) -> Result<RowStream> {
        match self {
            Self::Empty(p) => p.stream(),
            Self::Filter(p) => p.stream(),
            Self::Projection(p) => p.stream(),
            _ => Err(FloppyError::NotImplemented(format!(
                "physical sql not implemented: {:?}",
                self
            ))),
        }
    }
}

/// Trait for iterator execution.
/// The actual logic of different physical relational operators is implemented
/// in various `RowStream`
pub type RowStream = Pin<Box<dyn Stream<Item = Result<Row>>>>;
