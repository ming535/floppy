use crate::expr::PhysicalExpr;
use common::error::{FloppyError, Result};
use common::row::Row;
use common::scalar::{Datum, ScalarType};
use common::schema::RelationDesc;
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
    cast_type: ScalarType,
}

impl TryCastExpr {
    pub fn new(
        expr: Arc<PhysicalExpr>,
        cast_type: ScalarType,
    ) -> Self {
        Self { expr, cast_type }
    }

    pub fn data_type(&self) -> Result<ScalarType> {
        Ok(self.cast_type.clone())
    }

    pub fn evaluate(&self, tuple: &Row) -> Result<Datum> {
        let from_value = self.expr.evaluate(tuple)?;
        match (&from_value, &self.cast_type) {
            (Datum::Int32(Some(v1)), ScalarType::Int64) => {
                Ok(Datum::Int64(Some(*v1 as i64)))
            }
            _ => Err(FloppyError::NotImplemented(format!(
                "cast not implemented from {:?} to {:?}",
                from_value, self.cast_type
            ))),
        }
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
    input_schema: &RelationDesc,
    cast_type: ScalarType,
) -> Result<Arc<PhysicalExpr>> {
    let expr_type = expr.data_type(input_schema)?;
    if expr_type == cast_type {
        Ok(expr)
    } else {
        Ok(Arc::new(PhysicalExpr::TryCastExpr(
            TryCastExpr { expr, cast_type },
        )))
    }
}
