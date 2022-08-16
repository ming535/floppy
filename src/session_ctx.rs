use crate::common::error::Result;
use crate::logical_plan::planner::LogicalPlanner;
use crate::physical_plan::plan::PhysicalPlan;
use crate::physical_plan::planner::PhysicalPlanner;
use crate::store::{HeapStore, Store};
use crate::CatalogStore;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

pub struct SessionContext {
    logical_planner: LogicalPlanner,
    physical_planner: PhysicalPlanner,
}

impl SessionContext {
    pub fn new(
        catalog_store: Arc<dyn CatalogStore>,
        heap_store: Arc<dyn HeapStore>,
    ) -> Self {
        Self {
            logical_planner: LogicalPlanner::new(
                catalog_store.clone(),
            ),
            physical_planner: PhysicalPlanner::new(
                heap_store.clone(),
            ),
        }
    }

    pub fn create_plan(
        &self,
        sql: &str,
    ) -> Result<Vec<PhysicalPlan>> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        let mut physical_plans = vec![];
        for s in statements {
            let plan = self
                .logical_planner
                .statement_to_plan(s)?;
            let mut plan = self
                .physical_planner
                .create_physical_plan(&plan)?;
            physical_plans.push(plan)
        }
        Ok(physical_plans)
    }
}
