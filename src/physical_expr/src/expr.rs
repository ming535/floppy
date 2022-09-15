use common::error::{FloppyError, Result};

use crate::binary_expr::BinaryExpr;
use crate::column::Column;
use crate::try_cast::TryCastExpr;
use common::relation::ColumnRef;
use common::relation::RelationDesc;
use common::relation::Row;
use common::scalar::{Datum, ScalarType};
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    /// A column reference
    Column(ColumnRef),
    /// A constant value
    Literal(Datum),
    BinaryExpr(BinaryExpr),
    TryCastExpr(TryCastExpr),
}

impl PhysicalExpr {
    pub fn data_type(&self, input_rel: &RelationDesc) -> Result<ScalarType> {
        match self {
            Self::Column(c) => Ok(input_rel.column_type(c.idx)?.scalar_type().clone()),
            Self::Literal(v) => Ok(v.data_type()),
            Self::BinaryExpr(b) => b.data_type(input_rel),
            Self::TryCastExpr(t) => t.data_type(),
        }
    }

    pub fn evaluate(&self, row: &Row) -> Result<Datum> {
        match self {
            Self::Column(c) => row.value(c.idx),
            Self::Literal(v) => Ok(v.clone()),
            Self::TryCastExpr(t) => t.evaluate(row),
            Self::BinaryExpr(e) => e.evaluate(row),
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
