use std::fmt;
use std::fmt::Formatter;

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
    Float64(Option<f64>),
    Utf8(Option<String>),
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
            Self::Float64(e) => format_option!(f, e)?,
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
            Self::Float64(_) => write!(f, "Float64({})", self),
            Self::Utf8(None) => write!(f, "Utf8({})", self),
            Self::Utf8(Some(_)) => write!(f, "Utf8(\"{}\")", self),
        }
    }
}
