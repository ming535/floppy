use crate::analyzer;
use crate::context::StatementContext;
use crate::physical_plan::planner;
use crate::PhysicalPlan;
use common::error::Result;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub fn plan(scx: &StatementContext, sql: &str) -> Result<PhysicalPlan> {
    let dialect = PostgreSqlDialect {};
    let statement = &Parser::parse_sql(&dialect, sql)?[0];

    let logical_plan = analyzer::transform_statement(scx, statement)?;
    planner::plan(scx, logical_plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::relation::Row;
    use common::scalar::Datum;
    use futures::StreamExt;
    use std::sync::Arc;
    use test_util::seed;

    #[tokio::test]
    async fn test_select_no_relation() -> Result<()> {
        let (catalog, _) = seed::seed(&vec![])?;
        let scx = StatementContext::new(Arc::new(catalog));
        let mut plan = plan(&scx, "SELECT 1 + 2")?;
        let mut stream = plan.stream().expect("no error");
        let row = stream
            .next()
            .await
            .expect("have one row")
            .expect("no error");
        assert_eq!(row, Row::new(vec![Datum::Int32(3)]));

        let stream = plan.stream().expect("no error");
        // assert_eq!(stream.is_none(), true);
        Ok(())
    }

    #[test]
    fn test_simple_scan() -> Result<()> {
        // let r = Row::new(vec![Datum::Int32(1), Datum::Int32(2)]);
        // let (catalog, _) = seed::seed(&vec![r.clone()])?;
        // let scx = StatementContext::new(Arc::new(catalog));
        // let mut sql = plan(&scx, "SELECT * FROM test")?;
        // let row = sql
        //     .evaluate()
        //     .expect("no error")
        //     .expect("should have one row");
        // assert_eq!(row, r.clone());
        // let row = sql.evaluate().expect("no error");
        // assert_eq!(row.is_none(), true);
        Ok(())
    }
}
