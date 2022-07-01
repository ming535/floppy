use crate::logical_expr::column::Column;
use crate::logical_expr::value::Value;
use crate::logical_plan::operator::Operator;
use std::fmt;
use std::fmt::Formatter;

#[derive(Clone)]
pub enum Expr {
    /// A column reference
    Column(Column),
    /// A constant value
    Literal(Value),
    /// A binary expression
    BinaryExpr {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(c) => write!(f, "{}", c),
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::BinaryExpr { left, op, right } => {
                write! {f, "{:?} {} {:?}", left, op, right}
            }
        }
    }
}
