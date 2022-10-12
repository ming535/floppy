use crate::plan::{EmptyRelation, Filter, LogicalPlan, Projection, TableScan};
use common::error::FloppyError;
use common::error::Result;
use common::relation::{RelationDesc, RelationDescRef};
use plan::expr::ScalarExpr;
use std::sync::Arc;

#[derive(Default)]
pub struct LogicalPlanBuilder {
    plan: Option<LogicalPlan>,
}

/// LogicalPlanBuilder is used in LogicalPlanner to build logical plan.
/// It is also used in test cases so that we can easily construct
/// the tree of logical plan.
impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: LogicalPlan) -> Self {
        Self { plan: Some(plan) }
    }

    /// Create an empty relation
    pub fn empty_relation() -> Self {
        Self::from(LogicalPlan::EmptyRelation(EmptyRelation {
            rel: Arc::new(RelationDesc::empty()),
        }))
    }

    /// Scan from a relation
    pub fn scan(
        table_name: &str,
        rel: RelationDescRef,
        filters: Vec<ScalarExpr>,
    ) -> Result<Self> {
        let plan = LogicalPlan::TableScan(TableScan {
            table_name: table_name.to_string(),
            projected_rel: rel,
            filters,
        });
        Ok(Self { plan: Some(plan) })
    }

    pub fn plan(&self) -> Result<&LogicalPlan> {
        let plan = self
            .plan
            .as_ref()
            .ok_or(FloppyError::Internal("plan is none".to_string()))?;
        Ok(plan)
    }

    pub fn build(&self) -> Result<LogicalPlan> {
        let plan = self
            .plan
            .as_ref()
            .ok_or(FloppyError::Internal("plan is none".to_string()))?;
        Ok(plan.clone())
    }

    pub fn project(&self, expr: Vec<ScalarExpr>) -> Result<Self> {
        let input = self
            .plan
            .as_ref()
            .ok_or(FloppyError::Internal("plan is none".to_string()))?;

        let plan = LogicalPlan::Projection(Projection {
            expr,
            input: Arc::new(input.clone()),
            rel: input.relation_desc().clone(),
        });

        Ok(Self { plan: Some(plan) })
    }

    pub fn filter(&self, expr: ScalarExpr) -> Result<Self> {
        let input = self
            .plan
            .as_ref()
            .ok_or(FloppyError::Internal("plan is none".to_string()))?;

        let plan = LogicalPlan::Filter(Filter {
            predicate: expr,
            input: Arc::new(input.clone()),
        });

        Ok(Self { plan: Some(plan) })
    }
}
