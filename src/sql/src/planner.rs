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
    use crate::context::ExecutionContext;
    use common::relation::Row;
    use common::scalar::Datum;
    use futures::StreamExt;
    use std::sync::Arc;
    use test_util::seeder;

    #[tokio::test]
    async fn test_select_no_relation() -> Result<()> {
        let (catalog, table_store) = seeder::seed_catalog_and_table(&vec![])?;
        let scx = StatementContext::new(catalog.clone());
        let exec_ctx = ExecutionContext::new(table_store.clone());
        let mut plan = plan(&scx, "SELECT 1 + 2")?;
        let mut stream = plan.stream(Arc::new(exec_ctx)).expect("no error");
        let row = stream
            .next()
            .await
            .expect("have a result")
            .expect("no error");
        assert_eq!(row, Row::new(vec![Datum::Int32(3)]));
        assert_eq!(stream.next().await.is_none(), true);
        Ok(())
    }

    #[tokio::test]
    async fn test_simple_scan() -> Result<()> {
        let r1 = Row::new(vec![Datum::Int32(1), Datum::Int32(2)]);
        let r2 = Row::new(vec![Datum::Int32(3), Datum::Int32(4)]);
        let (catalog, table_store) = seeder::seed_catalog_and_table(&vec![r1.clone(), r2.clone()])?;
        let scx = StatementContext::new(catalog.clone());
        let exec_ctx = ExecutionContext::new(table_store.clone());
        let mut stream = plan(&scx, "SELECT * FROM test")?.stream(Arc::new(exec_ctx))?;
        let row = stream
            .next()
            .await
            .expect("have a result")
            .expect("no error");
        assert_eq!(row, r1);

        let row = stream
            .next()
            .await
            .expect("have a result")
            .expect("no error");
        assert_eq!(row, r2);
        Ok(())
    }
}
