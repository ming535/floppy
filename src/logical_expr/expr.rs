use crate::common::operator::Operator;
use crate::common::value::Value;
use crate::logical_expr::column::Column;
use std::fmt;
use std::fmt::Formatter;

#[derive(Clone)]
pub enum LogicalExpr {
    /// A column reference
    Column(Column),
    /// A constant value
    Literal(Value),
    /// A binary expression
    BinaryExpr {
        left: Box<LogicalExpr>,
        op: Operator,
        right: Box<LogicalExpr>,
    },
}

impl fmt::Debug for LogicalExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LogicalExpr::Column(c) => write!(f, "{}", c),
            LogicalExpr::Literal(v) => write!(f, "{:?}", v),
            LogicalExpr::BinaryExpr { left, op, right } => {
                write! {f, "{:?} {} {:?}", left, op, right}
            }
        }
    }
}
