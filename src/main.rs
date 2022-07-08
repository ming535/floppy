extern crate core;

mod catalog;
mod common;
mod logical_expr;
mod logical_plan;
mod physical_expr;
mod physical_plan;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let dialect = GenericDialect {}; // or AnsiDialect

    let sql = "SELECT a, b \
           FROM table_1 \
           WHERE a > b AND b < 100";

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    println!("AST: {:?}", ast);
}
