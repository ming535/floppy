use crate::context::{ExprContext, StatementContext};
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::pri_scan::PriKeyScanExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::{Expr, LogicalPlan, PhysicalPlan};
use catalog::names::FullObjectName;
use common::error::{FloppyError, Result};
use common::relation::{GlobalId, RelationDesc};
use std::sync::Arc;

/// todo! think about the parameter type of StatementContext
/// use & or Arc, or any other type?
pub(crate) fn plan(
    scx: &StatementContext,
    logical_plan: LogicalPlan,
) -> Result<PhysicalPlan> {
    match logical_plan {
        LogicalPlan::Empty => Ok(PhysicalPlan::Empty(EmptyExec::new())),
        LogicalPlan::Filter { input, predicate } => plan_filter(scx, *input, predicate),
        LogicalPlan::Projection {
            exprs,
            input,
            rel_desc,
        } => plan_projection(scx, *input, exprs, rel_desc),
        LogicalPlan::Table {
            table_id,
            rel_desc,
            name,
        } => plan_table(table_id, rel_desc, name),
        _ => Err(FloppyError::NotImplemented(format!(
            "physical sql not implemented"
        ))),
    }
}

fn plan_filter(
    scx: &StatementContext,
    input: LogicalPlan,
    predicate: Expr,
) -> Result<PhysicalPlan> {
    let ecx = ExprContext {
        scx: Arc::new(scx.clone()),
        rel_desc: Arc::new(input.rel_desc()),
    };
    let input = plan(scx, input)?;
    Ok(PhysicalPlan::Filter(FilterExec {
        predicate,
        ecx,
        input: Box::new(input),
    }))
}

fn plan_projection(
    scx: &StatementContext,
    input: LogicalPlan,
    exprs: Vec<Expr>,
    rel_desc: RelationDesc,
) -> Result<PhysicalPlan> {
    let ecx = ExprContext {
        scx: Arc::new(scx.clone()),
        rel_desc: Arc::new(input.rel_desc()),
    };

    let input = plan(scx, input)?;

    Ok(PhysicalPlan::Projection(ProjectionExec {
        exprs,
        ecx,
        input: Box::new(input),
        rel_desc: Arc::new(rel_desc),
    }))
}

fn plan_table(
    table_id: GlobalId,
    rel_desc: RelationDesc,
    full_name: FullObjectName,
) -> Result<PhysicalPlan> {
    Ok(PhysicalPlan::PriKeyScan(PriKeyScanExec {
        table_id,
        rel_desc,
        full_name,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_select_no_relation() -> Result<()> {
        Ok(())
    }

    fn test_simple_scan() -> Result<()> {
        Ok(())
    }

    fn test_filter() -> Result<()> {
        Ok(())
    }
}
