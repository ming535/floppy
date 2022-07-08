use crate::common::error::{FloppyError, Result};
use crate::common::operator::Operator;
use crate::common::tuple::Tuple;
use crate::common::value::Value;
use crate::physical_expr::binary_expr::BinaryExpr;
use crate::physical_expr::column::Column;

#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    /// A column reference
    Column(Column),
    /// A constant value
    Literal(Value),
    BinaryExpr(BinaryExpr),
}

impl PhysicalExpr {
    pub fn evaluate(&self, tuple: &Tuple) -> Result<Value> {
        match self {
            Self::Column(c) => tuple.value(c.index),
            Self::Literal(v) => Ok(v.clone()),
            _ => Err(FloppyError::NotImplemented(format!(
                "physical expr not implemented {:?}",
                self
            ))),
        }
    }
}
