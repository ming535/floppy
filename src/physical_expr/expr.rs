use crate::common::value::Value;
use crate::logical_plan::operator::Operator;
use crate::physical_expr::column::Column;

#[derive(Clone)]
pub enum PhysicalExpr {
    /// A column reference
    Column(Column),
    /// A constant value
    Literal(Value),
    BinaryExpr {
        left: Box<PhysicalExpr>,
        op: Operator,
        right: Box<PhysicalExpr>,
    },
}
