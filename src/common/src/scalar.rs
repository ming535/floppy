use crate::error::FloppyError;
use crate::error::Result;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::{fmt, ops};

/// A single value.
#[derive(Clone)]
pub enum Datum {
    /// An unknown value.
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
}

impl Datum {
    pub fn data_type(&self) -> ScalarType {
        match self {
            Self::Null => ScalarType::Null,
            Self::Boolean(_) => ScalarType::Boolean,
            Self::Int8(_) => ScalarType::Int8,
            Self::Int16(_) => ScalarType::Int16,
            Self::Int32(_) => ScalarType::Int32,
            Self::Int64(_) => ScalarType::Int64,
            Self::UInt8(_) => ScalarType::UInt8,
            Self::UInt16(_) => ScalarType::UInt16,
            Self::UInt32(_) => ScalarType::UInt32,
            Self::UInt64(_) => ScalarType::UInt64,
            Self::Utf8(_) => ScalarType::Utf8,
        }
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        use Datum::*;
        match (self, other) {
            (Null, _) => false,
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1.eq(v2),
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1.eq(v2),
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1.eq(v2),
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
            (Utf8(v1), Utf8(v2)) => v1.eq(v2),
            (Utf8(_), _) => false,
        }
    }
}

impl PartialOrd for Datum {
    fn partial_cmp(
        &self,
        other: &Self,
    ) -> Option<Ordering> {
        use Datum::*;

        match (self, other) {
            (Null, _) => None,
            (Boolean(v1), Boolean(v2)) => {
                v1.partial_cmp(v2)
            }
            (Boolean(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_v_), _) => None,
            (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
            (UInt8(_), _) => None,
            (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
            (UInt16(_), _) => None,
            (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
            (UInt32(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
            (Utf8(v1), Utf8(v2)) => v1.partial_cmp(v2),
            (Utf8(_), _) => None,
        }
    }
}

impl Eq for Datum {}

impl ops::Add<Datum> for Datum {
    type Output = Result<Datum>;

    fn add(self, rhs: Datum) -> Self::Output {
        let left_data_type = self.data_type();
        let right_data_type = rhs.data_type();
        if left_data_type != right_data_type {
            return Err(FloppyError::Internal(format!(
                "left and right should be the same type left: {:?}, right: {:?}",
                left_data_type, right_data_type
            )));
        }

        if !left_data_type.is_numeric()
            || !right_data_type.is_numeric()
        {
            return Err(FloppyError::Internal(format!(
                "'add' is only supported for numeric types, but we got left: {:?}, right: {:?}",
                left_data_type,
                right_data_type
            )));
        }
        match (self, rhs) {
            (
                Self::Int8(Some(left)),
                Self::Int8(Some(right)),
            ) => Ok(Self::Int8(Some(left + right))),
            (
                Self::Int16(Some(left)),
                Self::Int16(Some(right)),
            ) => Ok(Self::Int16(Some(left + right))),
            (
                Self::Int32(Some(left)),
                Self::Int32(Some(right)),
            ) => Ok(Self::Int32(Some(left + right))),
            (
                Self::Int64(Some(left)),
                Self::Int64(Some(right)),
            ) => Ok(Self::Int64(Some(left + right))),
            (
                Self::UInt8(Some(left)),
                Self::UInt8(Some(right)),
            ) => Ok(Self::UInt8(Some(left + right))),
            (
                Self::UInt16(Some(left)),
                Self::UInt16(Some(right)),
            ) => Ok(Self::UInt16(Some(left + right))),
            (
                Self::UInt32(Some(left)),
                Self::UInt32(Some(right)),
            ) => Ok(Self::UInt32(Some(left + right))),
            (
                Self::UInt64(Some(left)),
                Self::UInt64(Some(right)),
            ) => Ok(Self::UInt64(Some(left + right))),
            _ => {
                return Err(FloppyError::Internal(format!(
                    "'add' is only supported for numeric types, but we got left: {:?}, right: {:?}",
                    left_data_type,
                    right_data_type
                )));
            }
        }
    }
}

impl ops::Sub<Datum> for Datum {
    type Output = Result<Datum>;

    fn sub(self, rhs: Datum) -> Self::Output {
        let left_data_type = self.data_type();
        let right_data_type = rhs.data_type();

        if !left_data_type.is_numeric()
            || !right_data_type.is_numeric()
        {
            return Err(FloppyError::Internal(format!(
                "'sub' is only supported for numeric types, but we got left: {:?}, right: {:?}",
                left_data_type,
                right_data_type
            )));
        }

        if left_data_type != right_data_type {
            return Err(FloppyError::Internal(format!(
                "left and right should be the same type left: {:?}, right: {:?}",
                left_data_type, right_data_type
            )));
        }

        match (self, rhs) {
            (
                Self::Int8(Some(left)),
                Self::Int8(Some(right)),
            ) => Ok(Self::Int8(Some(left - right))),
            (
                Self::Int16(Some(left)),
                Self::Int16(Some(right)),
            ) => Ok(Self::Int16(Some(left - right))),
            (
                Self::Int32(Some(left)),
                Self::Int32(Some(right)),
            ) => Ok(Self::Int32(Some(left - right))),
            (
                Self::Int64(Some(left)),
                Self::Int64(Some(right)),
            ) => Ok(Self::Int64(Some(left - right))),
            (
                Self::UInt8(Some(left)),
                Self::UInt8(Some(right)),
            ) => Ok(Self::UInt8(Some(left - right))),
            (
                Self::UInt16(Some(left)),
                Self::UInt16(Some(right)),
            ) => Ok(Self::UInt16(Some(left - right))),
            (
                Self::UInt32(Some(left)),
                Self::UInt32(Some(right)),
            ) => Ok(Self::UInt32(Some(left - right))),
            (
                Self::UInt64(Some(left)),
                Self::UInt64(Some(right)),
            ) => Ok(Self::UInt64(Some(left - right))),
            _ => {
                return Err(FloppyError::Internal(format!(
                    "'sub' is only supported for numeric types, but we got left: {:?}, right: {:?}",
                    left_data_type,
                    right_data_type
                )));
            }
        }
    }
}

impl Datum {
    pub fn logical_and(
        &self,
        rhs: &Datum,
    ) -> Result<Datum> {
        match (self, rhs) {
            (Self::Boolean(Some(first)), Self::Boolean(Some(second))) => {
                if *first && *second {
                    Ok(Self::Boolean(Some(true)))
                } else {
                    Ok(Self::Boolean(Some(false)))
                }
            }
            _ => {
                Err(FloppyError::Internal(format!(
                    "'AND' is only supported for boolean type, but we got first {:?}, second: {:?}",
                    self,
                    rhs
                )))
            }
        }
    }

    pub fn logical_or(&self, rhs: &Datum) -> Result<Datum> {
        match (self, rhs) {
            (Self::Boolean(Some(first)), Self::Boolean(Some(second))) => {
                if *first {
                    Ok(Self::Boolean(Some(true)))
                } else {
                    Ok(Self::Boolean(Some(*second)))
                }
            },
            _ => {
                Err(FloppyError::Internal(format!(
                    "'OR' is only supported for boolean type, but we got first {:?}, second {:?}",
                    self, rhs
                )))
            }
        }
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "NULL"),
        }
    }};
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL")?,
            Self::Boolean(e) => format_option!(f, e)?,
            Self::Int8(e) => format_option!(f, e)?,
            Self::Int16(e) => format_option!(f, e)?,
            Self::Int32(e) => format_option!(f, e)?,
            Self::Int64(e) => format_option!(f, e)?,
            Self::UInt8(e) => format_option!(f, e)?,
            Self::UInt16(e) => format_option!(f, e)?,
            Self::UInt32(e) => format_option!(f, e)?,
            Self::UInt64(e) => format_option!(f, e)?,
            Self::Utf8(e) => format_option!(f, e)?,
        }
        Ok(())
    }
}

impl fmt::Debug for Datum {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(_) => {
                write!(f, "Boolean({})", self)
            }
            Self::Int8(_) => write!(f, "Int8({})", self),
            Self::Int16(_) => write!(f, "Int16({})", self),
            Self::Int32(_) => write!(f, "Int32({})", self),
            Self::Int64(_) => write!(f, "Int64({})", self),
            Self::UInt8(_) => write!(f, "UInt8({})", self),
            Self::UInt16(_) => write!(f, "UInt8({})", self),
            Self::UInt32(_) => {
                write!(f, "UInt32({})", self)
            }
            Self::UInt64(_) => {
                write!(f, "UInt64({})", self)
            }
            Self::Utf8(None) => write!(f, "Utf8({})", self),
            Self::Utf8(Some(_)) => {
                write!(f, "Utf8(\"{}\")", self)
            }
        }
    }
}

/// ScalarType defines the type of a [`Datum`]
///
/// There is a direct correspondence between `Datum` variants and `ScalarType`
/// variants.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    /// A variable-length string in Unicode with UTF-8 encoding.
    Utf8,
}

impl ScalarType {
    pub fn is_signed_numeric(&self) -> bool {
        matches!(
            self,
            ScalarType::Int8
                | ScalarType::Int16
                | ScalarType::Int32
                | ScalarType::Int64
        )
    }

    pub fn is_unsigned_numeric(&self) -> bool {
        matches!(
            self,
            ScalarType::UInt8
                | ScalarType::UInt16
                | ScalarType::UInt32
                | ScalarType::UInt64
        )
    }

    pub fn is_numeric(&self) -> bool {
        self.is_signed_numeric()
            || self.is_unsigned_numeric()
    }
}
