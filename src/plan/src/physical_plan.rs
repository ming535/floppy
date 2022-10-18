mod empty;
mod filter;
mod index_scan;
mod phys_planner;
mod projection;
mod table_scan;

use crate::physical_plan::index_scan::SecondaryIndexScan;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::table_scan::{FullTableScanExec, PrimaryIndexTableScanExec};
use common::error::{FloppyError, Result};
use common::relation::Row;
use empty::EmptyExec;
use filter::FilterExec;

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
