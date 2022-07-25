use crate::common::error::FloppyError;
use crate::common::error::Result;
use crate::common::schema::DataType;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::{fmt, ops};

#[derive(Clone)]
pub enum Value {
    /// represents `DataType::Null`
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

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt8(_) => DataType::UInt8,
            Self::UInt16(_) => DataType::UInt16,
            Self::UInt32(_) => DataType::UInt32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Utf8(_) => DataType::Utf8,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Null, Null) => true,
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

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Value::*;

        match (self, other) {
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
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

impl Eq for Value {}

impl ops::Add<Value> for Value {
    type Output = Result<Value>;

    fn add(self, rhs: Value) -> Self::Output {
        let left_data_type = self.data_type();
        let right_data_type = rhs.data_type();
        if left_data_type != right_data_type {
            return Err(FloppyError::Internal(format!(
                "left and right should be the same type left: {:?}, right: {:?}",
                left_data_type, right_data_type
            )));
        }

        if !left_data_type.is_numeric() || !right_data_type.is_numeric() {
            return Err(FloppyError::Internal(format!(
                "'add' is only supported for numeric types, but we got left: {:?}, right: {:?}",
                left_data_type,
                right_data_type
            )));
        }
        match (self, rhs) {
            (Self::Int8(Some(left)), Self::Int8(Some(right))) => {
                Ok(Self::Int8(Some(left + right)))
            }
            (Self::Int16(Some(left)), Self::Int16(Some(right))) => {
                Ok(Self::Int16(Some(left + right)))
            }
            (Self::Int32(Some(left)), Self::Int32(Some(right))) => {
                Ok(Self::Int32(Some(left + right)))
            }
            (Self::Int64(Some(left)), Self::Int64(Some(right))) => {
                Ok(Self::Int64(Some(left + right)))
            }
            (Self::UInt8(Some(left)), Self::UInt8(Some(right))) => {
                Ok(Self::UInt8(Some(left + right)))
            }
            (Self::UInt16(Some(left)), Self::UInt16(Some(right))) => {
                Ok(Self::UInt16(Some(left + right)))
            }
            (Self::UInt32(Some(left)), Self::UInt32(Some(right))) => {
                Ok(Self::UInt32(Some(left + right)))
            }
            (Self::UInt64(Some(left)), Self::UInt64(Some(right))) => {
                Ok(Self::UInt64(Some(left + right)))
            }
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

impl ops::Sub<Value> for Value {
    type Output = Result<Value>;

    fn sub(self, rhs: Value) -> Self::Output {
        let left_data_type = self.data_type();
        let right_data_type = rhs.data_type();

        if !left_data_type.is_numeric() || !right_data_type.is_numeric() {
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
            (Self::Int8(Some(left)), Self::Int8(Some(right))) => {
                Ok(Self::Int8(Some(left - right)))
            }
            (Self::Int16(Some(left)), Self::Int16(Some(right))) => {
                Ok(Self::Int16(Some(left - right)))
            }
            (Self::Int32(Some(left)), Self::Int32(Some(right))) => {
                Ok(Self::Int32(Some(left - right)))
            }
            (Self::Int64(Some(left)), Self::Int64(Some(right))) => {
                Ok(Self::Int64(Some(left - right)))
            }
            (Self::UInt8(Some(left)), Self::UInt8(Some(right))) => {
                Ok(Self::UInt8(Some(left - right)))
            }
            (Self::UInt16(Some(left)), Self::UInt16(Some(right))) => {
                Ok(Self::UInt16(Some(left - right)))
            }
            (Self::UInt32(Some(left)), Self::UInt32(Some(right))) => {
                Ok(Self::UInt32(Some(left - right)))
            }
            (Self::UInt64(Some(left)), Self::UInt64(Some(right))) => {
                Ok(Self::UInt64(Some(left - right)))
            }
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

impl Value {
    pub fn logical_and(&self, rhs: &Value) -> Result<Value> {
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

    pub fn logical_or(&self, rhs: &Value) -> Result<Value> {
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

impl fmt::Display for Value {
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

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(_) => write!(f, "Boolean({})", self),
            Self::Int8(_) => write!(f, "Int8({})", self),
            Self::Int16(_) => write!(f, "Int16({})", self),
            Self::Int32(_) => write!(f, "Int32({})", self),
            Self::Int64(_) => write!(f, "Int64({})", self),
            Self::UInt8(_) => write!(f, "UInt8({})", self),
            Self::UInt16(_) => write!(f, "UInt8({})", self),
            Self::UInt32(_) => write!(f, "UInt32({})", self),
            Self::UInt64(_) => write!(f, "UInt64({})", self),
            Self::Utf8(None) => write!(f, "Utf8({})", self),
            Self::Utf8(Some(_)) => write!(f, "Utf8(\"{}\")", self),
        }
    }
}
