use crate::adt::char::CharLength;
use crate::adt::varchar::VarCharMaxLength;
use crate::error::{FloppyError, Result};
use crate::relation::ColumnType;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::{fmt, ops};

/// A single value.
///
/// Note that `Datum` must always derive [`Eq`] to enforce
/// equality with `repr::Row`.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Datum {
    Boolean(bool),
    /// A 16-bit signed integer.
    Int16(i16),
    /// A 32-bit signed integer.
    Int32(i32),
    /// A 64-bit signed integer.
    Int64(i64),
    /// A 32-bit unsigned integer.
    UInt32(u32),
    /// A sequence of Unicode codepoints encoded as UTF-8.
    /// todo! consider using String('a& str)
    String(String),
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
            (Self::Int16(d1), Self::Int16(d2)) => d1.checked_add(d2).map_or_else(
                || Err(FloppyError::EvalExpr(format!("integer over flow"))),
                |v| Ok(Datum::Int16(v)),
            ),
            (Self::Int32(d1), Self::Int32(d2)) => d1.checked_add(d2).map_or_else(
                || Err(FloppyError::EvalExpr(format!("integer over flow"))),
                |v| Ok(Datum::Int32(v)),
            ),
            (Self::Int64(d1), Self::Int64(d2)) => d1.checked_add(d2).map_or_else(
                || Err(FloppyError::EvalExpr(format!("integer over flow"))),
                |v| Ok(Datum::Int64(v)),
            ),
            _ => Err(FloppyError::Internal(format!(
                "mismatched type for addition"
            ))),
        }
    }
}

impl ops::Sub for Datum {
    type Output = Result<Datum>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Int16(d1), Self::Int16(d2)) => d1.checked_sub(d2).map_or_else(
                || Err(FloppyError::EvalExpr(format!("integer over flow"))),
                |v| Ok(Datum::Int16(v)),
            ),
            (Self::Int32(d1), Self::Int32(d2)) => d1.checked_sub(d2).map_or_else(
                || Err(FloppyError::EvalExpr(format!("integer over flow"))),
                |v| Ok(Datum::Int32(v)),
            ),
            (Self::Int64(d1), Self::Int64(d2)) => d1.checked_sub(d2).map_or_else(
                || Err(FloppyError::EvalExpr(format!("integer over flow"))),
                |v| Ok(Datum::Int64(v)),
            ),
            _ => Err(FloppyError::Internal(format!(
                "mismatched type for addition"
            ))),
        }
    }
}

// impl PartialEq for Datum {
//     fn eq(&self, other: &Self) -> bool {
//         match (self, other) {
//             (Self::Boolean(d1), Self::Boolean(d2)) => d1 == d2,
//             (Self::Int16(d1), Self::Int16(d2)) => d1 == d2,
//             (Self::Int32(d1), Self::Int32(d2)) => d1 == d2,
//             (Self::UInt32(d1), Self::UInt32(d2)) => d1 == d2,
//             (Self::String(d1), Self::String(d2)) => d1 == d2,
//             _ => false,
//         }
//     }
// }
//
// impl Eq for Datum {}
//
// impl PartialOrd for Datum {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         match (self, other) {
//             (Self::Int16(d1), Self::Int16(d2)) => d1.partial_cmp(d2),
//             (Self::Int32(d1), Self::Int32(d2)) => d1.partial_cmp(d2),
//             (Self::Int64(d1), Self::Int64(d2)) => d1.partial_cmp(d2),
//             (Self::UInt32(d1), Self::UInt32(d2)) => d1.partial_cmp(d2),
//             (Self::String(d1), Self::String(d2)) => d1.partial_cmp(d2),
//             _ => None,
//         }
//     }
// }

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
            Self::Int16(e) => write!(f, "{}", e),
            Self::Int32(e) => write!(f, "{}", e),
            Self::Int64(e) => write!(f, "{}", e),
            Self::UInt32(e) => write!(f, "{}", e),
            Self::String(e) => write!(f, "{}", e),
            Self::Null => write!(f, "NULL"),
        }
    }
}

impl Datum {
    pub fn logical_and(&self, other: &Datum) -> Result<Datum> {
        match (self, other) {
            (Self::Boolean(d1), Self::Boolean(d2)) => Ok(Datum::Boolean(*d1 && *d2)),
            _ => Err(FloppyError::Internal(format!("AND type error"))),
        }
    }

    pub fn logical_or(&self, other: &Datum) -> Result<Datum> {
        match (self, other) {
            (Self::Boolean(d1), Self::Boolean(d2)) => Ok(Datum::Boolean(*d1 || *d2)),
            _ => Err(FloppyError::Internal(format!("OR type error"))),
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
    /// The type of [`Datum::Int16`].
    Int16,
    /// The type of [`Datum::Int32`].
    Int32,
    /// The type of [`Datum::Int64`].
    Int64,
    /// The type of [`Datum::String`].
    String,
    /// Stored as [`Datum::String`], but can optionally
    /// express a limit on the string's length.
    VarChar {
        max_length: Option<VarCharMaxLength>,
    },
    /// A PostgreSQL object identifier.
    Oid,
}

impl ScalarType {
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64
        )
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
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::String => write!(f, "String"),
            Self::VarChar { .. } => write!(f, "VarChar"),
            Self::Oid => write!(f, "Oid"),
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn datum_equal() {
        let d1 = Datum::Int32(2);
        let d2 = Datum::Int32(2);
        let d3 = Datum::Int32(3);
        let d4 = Datum::Int64(2);

        assert_eq!(d1 == d2, true);
        assert_eq!(d1 == d3, false);
        assert_eq!(d1 == d4, false);
    }

    #[test]
    fn test_order() {
        let d1 = Datum::String("abc".to_string());
        let d2 = Datum::String("b".to_string());
        let d3 = Datum::String("123456".to_string());
        assert_eq!(d2 > d1, true);
        assert_eq!(d1 > d3, true);
    }
}
