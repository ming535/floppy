use crate::common::error::FloppyError;
use crate::common::error::Result;
use crate::common::row::Row;
use crate::common::schema::{DataType, Schema};
use crate::common::value::Value;
use crate::physical_expr::expr::PhysicalExpr;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

/// TryCastExpr casts an expression to a specific data type and
/// returns NULL on invalid cast.
#[derive(Debug, Clone)]
pub struct TryCastExpr {
    /// The expression to cast
    expr: Arc<PhysicalExpr>,
    /// The data type to cast to
    cast_type: DataType,
}

impl TryCastExpr {
    pub fn new(
        expr: Arc<PhysicalExpr>,
        cast_type: DataType,
    ) -> Self {
        Self { expr, cast_type }
    }

    pub fn data_type(&self) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    pub fn evaluate(&self, tuple: &Row) -> Result<Value> {
        let v = self.expr.evaluate(tuple)?;
        todo!()
    }
}

impl fmt::Display for TryCastExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CAST({} AS {:?})",
            self.expr, self.cast_type
        )
    }
}

pub fn try_cast(
    expr: Arc<PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
) -> Result<Arc<PhysicalExpr>> {
    let expr_type = expr.data_type(input_schema)?;
    if expr_type == cast_type {
        Ok(expr.clone())
    } else {
        Ok(Arc::new(PhysicalExpr::TryCastExpr(
            TryCastExpr {
                expr: expr.clone(),
                cast_type,
            },
        )))
    }
}
