use crate::common::error::{FloppyError, Result};

use crate::common::row::Row;
use crate::common::schema::{DataType, Schema};
use crate::common::value::Value;
use crate::physical_expr::binary_expr::BinaryExpr;
use crate::physical_expr::column::Column;
use crate::physical_expr::try_cast::TryCastExpr;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    /// A column reference
    Column(Column),
    /// A constant value
    Literal(Value),
    BinaryExpr(BinaryExpr),
    TryCastExpr(TryCastExpr),
}

impl PhysicalExpr {
    pub fn data_type(
        &self,
        input_schema: &Schema,
    ) -> Result<DataType> {
        match self {
            Self::Column(c) => Ok(input_schema
                .field(c.index)
                .data_type()
                .clone()),
            Self::Literal(v) => Ok(v.data_type()),
            Self::BinaryExpr(b) => {
                b.data_type(input_schema)
            }
            Self::TryCastExpr(t) => t.data_type(),
        }
    }

    pub fn evaluate(&self, tuple: &Row) -> Result<Value> {
        match self {
            Self::Column(c) => tuple.value(c.index),
            Self::Literal(v) => Ok(v.clone()),
            Self::TryCastExpr(t) => t.evaluate(tuple),
            _ => Err(FloppyError::NotImplemented(format!(
                "physical expr not implemented {:?}",
                self
            ))),
        }
    }
}

impl fmt::Display for PhysicalExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(e) => write!(f, "{}", e),
            Self::Literal(e) => write!(f, "{}", e),
            Self::BinaryExpr(e) => write!(f, "{}", e),
            Self::TryCastExpr(e) => write!(f, "{}", e),
        }
    }
}
