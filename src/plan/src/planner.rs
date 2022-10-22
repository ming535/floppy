use crate::context::StatementContext;
use crate::logical_plan::log_planner::plan_statement;
use crate::physical_plan;
use crate::PhysicalPlan;
use common::error::Result;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub fn plan(scx: &StatementContext, sql: &str) -> Result<PhysicalPlan> {
    let dialect = PostgreSqlDialect {};
    let statement = &Parser::parse_sql(&dialect, sql)?[0];

    let logical_plan = plan_statement(scx, statement)?;
    physical_plan::phys_planner::plan(scx, logical_plan)
}
