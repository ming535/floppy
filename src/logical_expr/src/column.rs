use crate::expr::LogicalExpr;
use common::error::FloppyError;
use common::error::Result;
use common::schema::Column;
use common::schema::Schema;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

pub fn col(
    table_name: &str,
    col_name: &str,
) -> LogicalExpr {
    LogicalExpr::Column(Column {
        relation: Some(table_name.to_string()),
        name: col_name.to_string(),
    })
}
