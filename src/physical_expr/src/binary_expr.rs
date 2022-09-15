use crate::expr::PhysicalExpr;
use crate::try_cast::try_cast;
use common::error::{FloppyError, Result};
use common::operator::Operator;
use common::relation::RelationDesc;
use common::relation::Row;
use common::scalar::{Datum, ScalarType};
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    left: Arc<PhysicalExpr>,
    op: Operator,
    right: Arc<PhysicalExpr>,
}

impl BinaryExpr {
    pub fn new(left: Arc<PhysicalExpr>, op: Operator, right: Arc<PhysicalExpr>) -> Self {
        Self { left, op, right }
    }

    pub fn data_type(&self, input_schema: &RelationDesc) -> Result<ScalarType> {
        binary_operator_data_type(
            &self.left.data_type(input_schema)?,
            &self.op,
            &self.right.data_type(input_schema)?,
        )
    }

    pub fn evaluate(&self, tuple: &Row) -> Result<Datum> {
        let left_value = self.left.evaluate(tuple)?;
        let right_value = self.right.evaluate(tuple)?;
        let left_data_type = left_value.data_type();
        let right_data_type = right_value.data_type();

        // we already cast binary operand, so this should not happen.
        if left_data_type != right_data_type {
            return Err(FloppyError::Internal(format!(
                "Cannot evaluate binary expression {:?} with type {:?} and {:?}",
                self.op, left_data_type, right_data_type
            )));
        }

        match self.op {
            Operator::Eq => Ok(Datum::Boolean(Some(left_value == right_value))),
            Operator::NotEq => Ok(Datum::Boolean(Some(left_value != right_value))),
            Operator::Lt => Ok(Datum::Boolean(Some(left_value < right_value))),
            Operator::LtEq => Ok(Datum::Boolean(Some(left_value <= right_value))),
            Operator::Gt => Ok(Datum::Boolean(Some(left_value > right_value))),
            Operator::GtEq => Ok(Datum::Boolean(Some(left_value >= right_value))),
            Operator::Plus => left_value + right_value,
            Operator::Minus => left_value - right_value,
            Operator::And => left_value.logical_and(&right_value),
            Operator::Or => left_value.logical_or(&right_value),
        }
    }
}

impl fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

pub fn binary(
    lhs: Arc<PhysicalExpr>,
    op: Operator,
    rhs: Arc<PhysicalExpr>,
    input_schema: &RelationDesc,
) -> Result<Arc<PhysicalExpr>> {
    let (l, r) = binary_cast(lhs, &op, rhs, input_schema)?;
    Ok(Arc::new(PhysicalExpr::BinaryExpr(BinaryExpr::new(
        l, op, r,
    ))))
}

fn binary_cast(
    lhs: Arc<PhysicalExpr>,
    op: &Operator,
    rhs: Arc<PhysicalExpr>,
    input_schema: &RelationDesc,
) -> Result<(Arc<PhysicalExpr>, Arc<PhysicalExpr>)> {
    let lhs_type = &lhs.data_type(input_schema)?;
    let rhs_type = &rhs.data_type(input_schema)?;

    let result_type = coerce_types(lhs_type, op, rhs_type)?;
    Ok((
        try_cast(lhs, input_schema, result_type.clone())?,
        try_cast(rhs, input_schema, result_type)?,
    ))
}

fn binary_operator_data_type(
    lhs_type: &ScalarType,
    op: &Operator,
    rhs_type: &ScalarType,
) -> Result<ScalarType> {
    let result_type = coerce_types(lhs_type, op, rhs_type)?;

    match op {
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::And
        | Operator::Or => Ok(ScalarType::Boolean),
        Operator::Plus | Operator::Minus => Ok(result_type),
    }
}

/// Coercion rules for all binary operators. Returns the output type
/// of applying `op` to an argument of `lhs_type` and `rhs_type`.
fn coerce_types(
    lhs_type: &ScalarType,
    op: &Operator,
    rhs_type: &ScalarType,
) -> Result<ScalarType> {
    let result = match op {
        Operator::And | Operator::Or => match (lhs_type, rhs_type) {
            (ScalarType::Boolean, ScalarType::Boolean) => Some(ScalarType::Boolean),
            _ => None,
        },
        Operator::Eq | Operator::NotEq => comparison_eq_coercion(lhs_type, rhs_type),
        Operator::Lt | Operator::Gt | Operator::GtEq | Operator::LtEq => {
            comparison_order_coercion(lhs_type, rhs_type)
        }
        Operator::Plus | Operator::Minus => {
            mathematics_numerical_coercion(op, lhs_type, rhs_type)
        }
    };

    match result {
        None => Err(FloppyError::Plan(format!("'{:?} {} {:?}' can't be evaluated because there isn't a common type to coerce the types to", lhs_type, op, rhs_type))),
        Some(t) => Ok(t)
    }
}

fn comparison_eq_coercion(
    lhs_type: &ScalarType,
    rhs_type: &ScalarType,
) -> Option<ScalarType> {
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    comparison_binary_numeric_coercion(lhs_type, rhs_type)
        .or_else(|| string_numeric_coercion(lhs_type, rhs_type))
}

fn comparison_order_coercion(
    lhs_type: &ScalarType,
    rhs_type: &ScalarType,
) -> Option<ScalarType> {
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    comparison_binary_numeric_coercion(lhs_type, rhs_type)
}

fn mathematics_numerical_coercion(
    _op: &Operator,
    lhs_type: &ScalarType,
    rhs_type: &ScalarType,
) -> Option<ScalarType> {
    use common::scalar::ScalarType::*;

    if !both_numeric_or_null_and_numeric(lhs_type, rhs_type) {
        return None;
    }

    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

fn comparison_binary_numeric_coercion(
    lhs_type: &ScalarType,
    rhs_type: &ScalarType,
) -> Option<ScalarType> {
    use common::scalar::ScalarType::*;
    if !lhs_type.is_numeric() || !rhs_type.is_numeric() {
        return None;
    }

    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

fn string_numeric_coercion(
    lhs_type: &ScalarType,
    rhs_type: &ScalarType,
) -> Option<ScalarType> {
    use common::scalar::ScalarType::*;
    match (lhs_type, rhs_type) {
        (Utf8, _) if rhs_type.is_numeric() => Some(Utf8),
        (_, Utf8) if lhs_type.is_numeric() => Some(Utf8),
        _ => None,
    }
}

/// Determine if at least of one of lhs and rhs is numeric, and the other must be NULL or numeric
fn both_numeric_or_null_and_numeric(
    lhs_type: &ScalarType,
    rhs_type: &ScalarType,
) -> bool {
    match (lhs_type, rhs_type) {
        (_, ScalarType::Null) => lhs_type.is_numeric(),
        (ScalarType::Null, _) => rhs_type.is_numeric(),
        _ => lhs_type.is_numeric() && rhs_type.is_numeric(),
    }
}
