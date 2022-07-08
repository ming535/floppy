use crate::common::error::{FloppyError, Result};
use crate::common::schema::{DataType, Field, Schema, SchemaRef};
use crate::common::tuple::Tuple;
use crate::common::value::Value;
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::SendableTupleStream;
use futures::Stream;
use std::io::Empty;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone)]
pub enum PhysicalPlan {
    EmptyExec(EmptyExec),
    TableScanExec(TableScanExec),
    ProjectionExec(ProjectionExec),
    FilterExec(FilterExec),
}

impl PhysicalPlan {
    pub fn execute(&self) -> Result<SendableTupleStream> {
        match self {
            Self::EmptyExec(p) => p.execute(),
            _ => Err(FloppyError::NotImplemented(
                "physical expression not supported".to_owned(),
            )),
        }
    }
}

#[derive(Clone)]
pub struct TableScanExec {}

#[derive(Clone)]
pub struct FilterExec {}
