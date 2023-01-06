use crate::common::{
    error::{FloppyError, Result},
    relation::ColumnType,
};
use std::fmt::{self, Formatter};
use std::ops;

/// A single value.
///
/// Note that `Datum` must always derive [`Eq`] to enforce
/// equality with `repr::Row`.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Datum {
    Boolean(bool),
    /// A 64-bit signed integer.
    Int64(i64),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    Text(String),
    /// An unknown value.
    Null,
}

impl Datum {
    pub fn is_null(&self) -> bool {
        match self {
            Self::Null => true,
            _ => false,
        }
    }
}

impl ops::Add for Datum {
    type Output = Result<Datum>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Int64(d1), Self::Int64(d2)) => {
                d1.checked_add(d2).map_or_else(
                    || Err(FloppyError::EvalExpr("integer over flow".to_string())),
                    |v| Ok(Datum::Int64(v)),
                )
            }
            _ => Err(FloppyError::Internal("mismatched type for addition".to_string())),
        }
    }
}

impl ops::Sub for Datum {
    type Output = Result<Datum>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Int64(d1), Self::Int64(d2)) => {
                d1.checked_sub(d2).map_or_else(
                    || Err(FloppyError::EvalExpr("integer over flow".to_string())),
                    |v| Ok(Datum::Int64(v)),
                )
            }
            _ => Err(FloppyError::Internal("mismatched type for addition".to_string())),
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean(e) => {
                if *e {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Self::Int64(e) => write!(f, "{e}"),
            Self::Text(e) => write!(f, "{e}"),
            Self::Null => write!(f, "NULL"),
        }
    }
}

impl Datum {
    pub fn logical_and(&self, other: &Datum) -> Result<Datum> {
        match (self, other) {
            (Self::Boolean(d1), Self::Boolean(d2)) => {
                Ok(Datum::Boolean(*d1 && *d2))
            }
            _ => Err(FloppyError::Internal("AND type error".to_string())),
        }
    }

    pub fn logical_or(&self, other: &Datum) -> Result<Datum> {
        match (self, other) {
            (Self::Boolean(d1), Self::Boolean(d2)) => {
                Ok(Datum::Boolean(*d1 || *d2))
            }
            _ => Err(FloppyError::Internal("OR type error".to_string())),
        }
    }
}

/// The type of a [`Datum`].
///
/// There is a direct correspondence between `Datum`
/// variants and `ScalarType` variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarType {
    /// The type of [`Datum::Boolean`]
    Boolean,
    /// The type of [`Datum::Int64`]
    Int64,
    /// The type of [`Datum::String`]
    Text,
}

impl ScalarType {
    pub fn is_numeric(&self) -> bool {
        matches!(self, ScalarType::Int64)
    }

    /// Derive a `ColumnType` from `ScalarType`
    pub fn nullable(&self, b: bool) -> ColumnType {
        ColumnType {
            scalar_type: self.clone(),
            nullable: b,
        }
    }
}

impl fmt::Display for ScalarType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean => write!(f, "Boolean"),
            Self::Int64 => write!(f, "Int64"),
            Self::Text => write!(f, "Text"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datum_equal() {
        let d1 = Datum::Int64(2);
        let d2 = Datum::Int64(2);
        assert_eq!(d1 == d2, true);
    }

    #[test]
    fn test_order() {
        let d1 = Datum::Text("abc".to_string());
        let d2 = Datum::Text("b".to_string());
        let d3 = Datum::Text("123456".to_string());
        assert_eq!(d2 > d1, true);
        assert_eq!(d1 > d3, true);
    }
}
