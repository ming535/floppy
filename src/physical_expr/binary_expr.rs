use crate::common::error::Result;
use crate::common::operator::Operator;
use crate::common::tuple::Tuple;
use crate::common::value::Value;
use crate::physical_expr::expr::PhysicalExpr;

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    left: Box<PhysicalExpr>,
    op: Operator,
    right: Box<PhysicalExpr>,
}

impl BinaryExpr {
    pub fn evaluate(&self, tuple: &Tuple) -> Result<Value> {
        let left_value = self.left.evaluate(tuple)?;
        let right_value = self.right.evaluate(tuple)?;
        // match op {
        //     Operator::Eq => Ok(left_value == right_value),
        // }
        todo!()
    }
}
