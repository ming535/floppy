use crate::context::StatementContext;
use crate::{LogicalPlan, PhysicalPlan};
use common::error::Result;

pub fn plan(scx: &StatementContext, logical_plan: LogicalPlan) -> Result<PhysicalPlan> {
    todo!()
}
