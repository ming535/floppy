use crate::common::error::Result;
use crate::common::row::Row;
use crate::common::schema::SchemaRef;
use crate::physical_expr::expr::PhysicalExpr;
use crate::physical_plan::plan::PhysicalPlan;
use futures::{Stream, StreamExt};
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct ProjectionExec {
    pub expr: Vec<Arc<PhysicalExpr>>,
    pub input: Box<PhysicalPlan>,
    pub schema: SchemaRef,
}

impl ProjectionExec {
    pub fn next(&mut self) -> Result<Option<Row>> {
        let row = self.input.next()?;
        if let Some(row) = row {
            let values: Result<Vec<_>> = self
                .expr
                .iter()
                .map(|x| x.evaluate(&row))
                .collect();
            Ok(Some(Row::new(values?)))
        } else {
            Ok(None)
        }
    }
}
