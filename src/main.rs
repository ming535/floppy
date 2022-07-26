extern crate core;

mod common;
mod logical_expr;
mod logical_plan;
mod physical_expr;
mod physical_plan;
mod storage;
mod store;

use crate::common::row::Row;
use crate::common::schema::{DataType, Field, Schema};
use crate::common::value::Value;
use crate::storage::memory::MemoryEngine;
use crate::store::CatalogStore;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

fn main() {
    let dialect = GenericDialect {}; // or AnsiDialect

    let table_name = "test";

    let mem_engine = Arc::new(MemoryEngine::default());
    let schema = Schema::new(vec![
        Field::new(
            Some(table_name),
            "a",
            DataType::Int32,
            false,
        ),
        Field::new(
            Some(table_name),
            "b",
            DataType::Int32,
            false,
        ),
    ]);

    let data: Vec<Row> = (0..100)
        .map(|n| {
            Row::new(vec![
                Value::Int32(Some(n)),
                Value::Int32(Some(n * 2)),
            ])
        })
        .collect();
    mem_engine.insert_schema(table_name, &schema);
    mem_engine.seed(table_name, data.iter());

    let logical_planner =
        logical_plan::planner::LogicalPlanner::new(
            mem_engine.clone(),
        );
    let physical_planner =
        physical_plan::planner::PhysicalPlanner::new(
            mem_engine.clone(),
        );

    let sql = "SELECT a, b \
           FROM test \
           WHERE b > 100";

    let statements =
        Parser::parse_sql(&dialect, sql).unwrap();

    for s in statements {
        let plan =
            logical_planner.statement_to_plan(s).unwrap();
        let mut plan = physical_planner
            .create_physical_plan(&plan)
            .unwrap();
        while let Ok(Some(r)) = plan.next() {
            println!("row = {:?}", r);
        }
    }
}
