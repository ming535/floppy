use common::operator::Operator;
use common::relation::{ColumnRef, ColumnType};
use common::scalar::Datum;
use std::fmt;
use std::fmt::Formatter;

#[derive(Clone)]
pub enum LogicalExpr {
    /// A column reference
    Column(ColumnRef),
    /// A constant value
    Literal(Datum, ColumnType),
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
            LogicalExpr::Literal(v, _) => write!(f, "{:?}", v),
            LogicalExpr::BinaryExpr { left, op, right } => {
                write! {f, "{:?} {} {:?}", left, op, right}
            }
        }
    }
}
