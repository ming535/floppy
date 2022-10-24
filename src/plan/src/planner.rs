use crate::context::StatementContext;
use crate::logical_plan::log_planner;
use crate::physical_plan::phys_planner;
use crate::PhysicalPlan;
use common::error::Result;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub fn plan(scx: &StatementContext, sql: &str) -> Result<PhysicalPlan> {
    let dialect = PostgreSqlDialect {};
    let statement = &Parser::parse_sql(&dialect, sql)?[0];

    let logical_plan = log_planner::plan_statement(scx, statement)?;
    phys_planner::plan(scx, logical_plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::relation::Row;
    use common::scalar::Datum;
    use std::sync::Arc;
    use test_util::seed;

    #[test]
    fn test_select_no_relation() -> Result<()> {
        let (catalog, table) = seed::seed(&vec![])?;
        let scx = StatementContext::new(Arc::new(catalog));
        let mut plan = plan(&scx, "SELECT 1 + 2")?;
        let row = plan.next().expect("no error").expect("should have one row");
        assert_eq!(row, Row::new(vec![Datum::Int32(3)]));

        let row = plan.next().expect("no error");
        assert_eq!(row.is_none(), true);
        Ok(())
    }
}
