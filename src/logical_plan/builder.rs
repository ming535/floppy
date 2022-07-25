use crate::common::error::FloppyError;
use crate::common::error::Result;
use crate::common::schema::{Schema, SchemaRef};
use crate::logical_expr::expr::LogicalExpr;
use crate::logical_plan::plan::{
    EmptyRelation, Filter, LogicalPlan, Projection,
    TableScan,
};
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
        Self::from(LogicalPlan::EmptyRelation(
            EmptyRelation {
                schema: Arc::new(Schema::empty()),
            },
        ))
    }

    /// Scan from a relation
    pub fn scan(
        table_name: &str,
        schema: SchemaRef,
        filters: Vec<LogicalExpr>,
    ) -> Result<Self> {
        let plan = LogicalPlan::TableScan(TableScan {
            table_name: table_name.to_string(),
            projected_schema: schema,
            filters,
        });
        Ok(Self { plan: Some(plan) })
    }

    pub fn plan(&self) -> Result<&LogicalPlan> {
        let plan = self.plan.as_ref().ok_or(
            FloppyError::Internal("plan is none".to_string()),
        )?;
        Ok(plan)
    }

    pub fn build(&self) -> Result<LogicalPlan> {
        let plan = self.plan.as_ref().ok_or(
            FloppyError::Internal("plan is none".to_string()),
        )?;
        Ok(plan.clone())
    }

    pub fn project(
        &self,
        expr: Vec<LogicalExpr>,
    ) -> Result<Self> {
        let input = self.plan.as_ref().ok_or(
            FloppyError::Internal("plan is none".to_string()),
        )?;

        let plan = LogicalPlan::Projection(Projection {
            expr,
            input: Arc::new(input.clone()),
            schema: input.schema().clone(),
        });

        Ok(Self { plan: Some(plan) })
    }

    pub fn filter(
        &self,
        expr: LogicalExpr,
    ) -> Result<Self> {
        let input = self.plan.as_ref().ok_or(
            FloppyError::Internal("plan is none".to_string()),
        )?;

        let plan = LogicalPlan::Filter(Filter {
            predicate: expr,
            input: Arc::new(input.clone()),
        });

        Ok(Self { plan: Some(plan) })
    }
}
