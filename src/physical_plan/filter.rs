use crate::physical_expr::expr::PhysicalExpr;
use crate::physical_plan::plan::PhysicalPlan;
use std::sync::Arc;

pub struct FilterExec {
    pub predicate: Arc<PhysicalExpr>,
    pub input: Box<PhysicalPlan>,
}
