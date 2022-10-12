use crate::adt::char::CharLength;
use crate::adt::varchar::VarCharMaxLength;
use crate::relation::ColumnType;
use std::fmt;
use std::fmt::Formatter;

/// A single value.
///
/// Note that `Datum` must always derive [`Eq`] to enforce equality with
/// `repr::Row`.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
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

// impl PartialEq for Datum {
//     fn eq(&self, other: &Self) -> bool {
//         use Datum::*;
//         match (self, other) {
//             (Null, _) => false,
//             (Boolean(v1), Boolean(v2)) => v1.eq(v2),
//             (Boolean(_), _) => false,
//             (Int16(v1), Int16(v2)) => v1.eq(v2),
//             (Int16(_), _) => false,
//             (Int32(v1), Int32(v2)) => v1.eq(v2),
//             (Int32(_), _) => false,
//             (Int64(v1), Int64(v2)) => v1.eq(v2),
//             (Int64(_), _) => false,
//             (UInt32(v1), UInt32(v2)) => v1.eq(v2),
//             (UInt32(_), _) => false,
//         }
//     }
// }

// impl PartialOrd for Datum {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         use Datum::*;
//
//         match (self, other) {
//             (Null, _) => None,
//             (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
//             (Boolean(_), _) => None,
//             (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
//             (Int8(_), _) => None,
//             (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
//             (Int16(_), _) => None,
//             (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
//             (Int32(_), _) => None,
//             (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
//             (Int64(_v_), _) => None,
//             (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
//             (UInt8(_), _) => None,
//             (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
//             (UInt16(_), _) => None,
//             (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
//             (UInt32(_), _) => None,
//             (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
//             (UInt64(_), _) => None,
//             (Utf8(v1), Utf8(v2)) => v1.partial_cmp(v2),
//             (Utf8(_), _) => None,
//         }
//     }
// }

// impl Eq for Datum {}

// impl ops::Add<Datum> for Datum {
//     type Output = Result<Datum>;
//
//     fn add(self, rhs: Datum) -> Self::Output {
//         let left_data_type = self.data_type();
//         let right_data_type = rhs.data_type();
//         if left_data_type != right_data_type {
//             return Err(FloppyError::Internal(format!(
//                 "left and right should be the same type left: {:?}, right: {:?}",
//                 left_data_type, right_data_type
//             )));
//         }
//
//         if !left_data_type.is_numeric() || !right_data_type.is_numeric() {
//             return Err(FloppyError::Internal(format!(
//                 "'add' is only supported for numeric types, but we got left: {:?}, right: {:?}",
//                 left_data_type,
//                 right_data_type
//             )));
//         }
//         match (self, rhs) {
//             (Self::Int8(Some(left)), Self::Int8(Some(right))) => {
//                 Ok(Self::Int8(Some(left + right)))
//             }
//             (Self::Int16(Some(left)), Self::Int16(Some(right))) => {
//                 Ok(Self::Int16(Some(left + right)))
//             }
//             (Self::Int32(Some(left)), Self::Int32(Some(right))) => {
//                 Ok(Self::Int32(Some(left + right)))
//             }
//             (Self::Int64(Some(left)), Self::Int64(Some(right))) => {
//                 Ok(Self::Int64(Some(left + right)))
//             }
//             (Self::UInt8(Some(left)), Self::UInt8(Some(right))) => {
//                 Ok(Self::UInt8(Some(left + right)))
//             }
//             (Self::UInt16(Some(left)), Self::UInt16(Some(right))) => {
//                 Ok(Self::UInt16(Some(left + right)))
//             }
//             (Self::UInt32(Some(left)), Self::UInt32(Some(right))) => {
//                 Ok(Self::UInt32(Some(left + right)))
//             }
//             (Self::UInt64(Some(left)), Self::UInt64(Some(right))) => {
//                 Ok(Self::UInt64(Some(left + right)))
//             }
//             _ => {
//                 return Err(FloppyError::Internal(format!(
//                     "'add' is only supported for numeric types, but we got left: {:?}, right: {:?}",
//                     left_data_type,
//                     right_data_type
//                 )));
//             }
//         }
//     }
// }
//
// impl ops::Sub<Datum> for Datum {
//     type Output = Result<Datum>;
//
//     fn sub(self, rhs: Datum) -> Self::Output {
//         let left_data_type = self.data_type();
//         let right_data_type = rhs.data_type();
//
//         if !left_data_type.is_numeric() || !right_data_type.is_numeric() {
//             return Err(FloppyError::Internal(format!(
//                 "'sub' is only supported for numeric types, but we got left: {:?}, right: {:?}",
//                 left_data_type,
//                 right_data_type
//             )));
//         }
//
//         if left_data_type != right_data_type {
//             return Err(FloppyError::Internal(format!(
//                 "left and right should be the same type left: {:?}, right: {:?}",
//                 left_data_type, right_data_type
//             )));
//         }
//
//         match (self, rhs) {
//             (Self::Int8(Some(left)), Self::Int8(Some(right))) => {
//                 Ok(Self::Int8(Some(left - right)))
//             }
//             (Self::Int16(Some(left)), Self::Int16(Some(right))) => {
//                 Ok(Self::Int16(Some(left - right)))
//             }
//             (Self::Int32(Some(left)), Self::Int32(Some(right))) => {
//                 Ok(Self::Int32(Some(left - right)))
//             }
//             (Self::Int64(Some(left)), Self::Int64(Some(right))) => {
//                 Ok(Self::Int64(Some(left - right)))
//             }
//             (Self::UInt8(Some(left)), Self::UInt8(Some(right))) => {
//                 Ok(Self::UInt8(Some(left - right)))
//             }
//             (Self::UInt16(Some(left)), Self::UInt16(Some(right))) => {
//                 Ok(Self::UInt16(Some(left - right)))
//             }
//             (Self::UInt32(Some(left)), Self::UInt32(Some(right))) => {
//                 Ok(Self::UInt32(Some(left - right)))
//             }
//             (Self::UInt64(Some(left)), Self::UInt64(Some(right))) => {
//                 Ok(Self::UInt64(Some(left - right)))
//             }
//             _ => {
//                 return Err(FloppyError::Internal(format!(
//                     "'sub' is only supported for numeric types, but we got left: {:?}, right: {:?}",
//                     left_data_type,
//                     right_data_type
//                 )));
//             }
//         }
//     }
// }

// impl Datum {
//     pub fn logical_and(&self, rhs: &Datum) -> Result<Datum> {
//         match (self, rhs) {
//             (Self::Boolean(Some(first)), Self::Boolean(Some(second))) => {
//                 if *first && *second {
//                     Ok(Self::Boolean(Some(true)))
//                 } else {
//                     Ok(Self::Boolean(Some(false)))
//                 }
//             }
//             _ => {
//                 Err(FloppyError::Internal(format!(
//                     "'AND' is only supported for boolean type, but we got first {:?}, second: {:?}",
//                     self,
//                     rhs
//                 )))
//             }
//         }
//     }
//
//     pub fn logical_or(&self, rhs: &Datum) -> Result<Datum> {
//         match (self, rhs) {
//             (Self::Boolean(Some(first)), Self::Boolean(Some(second))) => {
//                 if *first {
//                     Ok(Self::Boolean(Some(true)))
//                 } else {
//                     Ok(Self::Boolean(Some(*second)))
//                 }
//             },
//             _ => {
//                 Err(FloppyError::Internal(format!(
//                     "'OR' is only supported for boolean type, but we got first {:?}, second {:?}",
//                     self, rhs
//                 )))
//             }
//         }
//     }
// }

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean(e) => write!(f, "{}", e)?,
            Self::Int16(e) => write!(f, "{}", e)?,
            Self::Int32(e) => write!(f, "{}", e)?,
            Self::Int64(e) => write!(f, "{}", e)?,
            Self::UInt32(e) => write!(f, "{}", e)?,
            Self::String(e) => write!(f, "{}", e)?,
            Self::Null => write!(f, "NULL")?,
        }
        Ok(())
    }
}

// impl fmt::Debug for Datum {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         match self {
//             Self::Null => write!(f, "NULL"),
//             Self::Boolean(_) => {
//                 write!(f, "Boolean({})", self)
//             }
//             Self::Int8(_) => write!(f, "Int8({})", self),
//             Self::Int16(_) => write!(f, "Int16({})", self),
//             Self::Int32(_) => write!(f, "Int32({})", self),
//             Self::Int64(_) => write!(f, "Int64({})", self),
//             Self::UInt8(_) => write!(f, "UInt8({})", self),
//             Self::UInt16(_) => write!(f, "UInt8({})", self),
//             Self::UInt32(_) => {
//                 write!(f, "UInt32({})", self)
//             }
//             Self::UInt64(_) => {
//                 write!(f, "UInt64({})", self)
//             }
//             Self::Utf8(None) => write!(f, "Utf8({})", self),
//             Self::Utf8(Some(_)) => {
//                 write!(f, "Utf8(\"{}\")", self)
//             }
//         }
//     }
// }

/// The type of a [`Datum`].
///
/// There is a direct correspondence between `Datum` variants and `ScalarType`
/// variants.
#[derive(Debug, Clone, PartialEq)]
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
    /// Stored as [`Datum::String`], but expresses a fixed-width, blank-padded
    /// string.
    ///
    /// Note that a `length` of `None` is used in special cases, such as
    /// creating lists.
    Char { length: Option<CharLength> },
    /// Stored as [`Datum::String`], but can optionally express a limit on the
    /// string's length.
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
            Self::Char { .. } => write!(f, "Char"),
            Self::VarChar { .. } => write!(f, "VarChar"),
            Self::Oid => write!(f, "Oid"),
        }
    }
}
