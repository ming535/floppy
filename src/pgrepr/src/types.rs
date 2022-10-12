use crate::value::numeric::{InvalidNumericMaxScaleError, NUMERIC_DATUM_MAX_PRECISION};
use common::adt::char::InvalidCharLengthError;
use common::adt::varchar::InvalidVarCharMaxLengthError;
use common::scalar::ScalarType;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;

/// Mirror of PostgreSQL's [`VARHDRSZ`] constant.
///
/// [`VARHDRSZ`]: https://github.com/postgres/postgres/blob/REL_14_0/src/include/c.h#L627
const VARHDRSZ: i32 = 4;

/// Mirror of PostgreSQL's [`MAX_INTERVAL_PRECISION`] constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/datatype/timestamp.h#L54>
const MAX_INTERVAL_PRECISION: i32 = 6;

/// Mirror of PostgreSQL's [`MAX_TIMESTAMP_PRECISION`] constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/datatype/timestamp.h#L53>
const MAX_TIMESTAMP_PRECISION: i32 = 6;

/// Mirror of PostgreSQL's [`MAX_TIME_PRECISION`] constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/utils/date.h#L51>
const MAX_TIME_PRECISION: i32 = 6;

/// The type of a [`Value`](crate::Value).
///
/// The [`Display`](fmt::Display) representation of a type is guaranteed to be
/// valid PostgreSQL syntax that names the type and any modifiers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Type {
    /// A variable-length multidimensional array of values.
    Array(Box<Type>),
    /// A boolean value.
    Bool,
    /// A byte array, i.e., a variable-length binary string.
    Bytea,
    /// A single-byte character.
    Char,
    /// A date.
    Date,
    /// A 4-byte floating point number.
    Float4,
    /// An 8-byte floating point number.
    Float8,
    /// A 2-byte signed integer.
    Int2,
    /// A 4-byte signed integer.
    Int4,
    /// An 8-byte signed integer.
    Int8,
    /// A time interval.
    Interval {
        /// Optional constraints on the type.
        constraints: Option<IntervalConstraints>,
    },
    /// A textual JSON blob.
    Json,
    /// A binary JSON blob.
    Jsonb,
    /// A sequence of homogeneous values.
    List(Box<Type>),
    /// A map with text keys and homogeneous values.
    Map {
        /// The type of the values in the map.
        value_type: Box<Type>,
    },
    /// An arbitrary precision number.
    Numeric {
        /// Optional constraints on the type.
        constraints: Option<NumericConstraints>,
    },
    /// An object identifier.
    Oid,
    /// A sequence of heterogeneous values.
    Record(Vec<Type>),
    /// A variable-length string.
    Text,
    /// A (usually) fixed-length string.
    BpChar {
        /// The length of the string.
        ///
        /// If unspecified, the type represents a variable-length string.
        length: Option<CharLength>,
    },
    /// A variable-length string with an optional limit.
    VarChar {
        /// An optional maximum length to enforce, in characters.
        max_length: Option<CharLength>,
    },
    /// A time of day without a day.
    Time {
        /// An optional precision for the fractional digits in the second field.
        precision: Option<TimePrecision>,
    },
    /// A time with a time zone.
    TimeTz {
        /// An optional precision for the fractional digits in the second field.
        precision: Option<TimePrecision>,
    },
    /// A date and time, without a timezone.
    Timestamp {
        /// An optional precision for the fractional digits in the second field.
        precision: Option<TimestampPrecision>,
    },
    /// A date and time, with a timezone.
    TimestampTz {
        /// An optional precision for the fractional digits in the second field.
        precision: Option<TimestampPrecision>,
    },
    /// A universally unique identifier.
    Uuid,
    /// A function name.
    RegProc,
    /// A type name.
    RegType,
    /// A class name.
    RegClass,
    /// A small int vector.
    Int2Vector,
}

/// An unpacked [`typmod`](Type::typmod) for a [`Type`].
pub trait TypeConstraint: fmt::Display {
    /// Unpacks the type constraint from a typmod value.
    fn from_typmod(typmod: i32) -> Result<Option<Self>, String>
    where
        Self: Sized;

    /// Packs the type constraint into a typmod value.
    fn into_typmod(&self) -> i32;
}

/// A length associated with [`Type::Char`] and [`Type::VarChar`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CharLength(i32);

impl TypeConstraint for CharLength {
    fn from_typmod(typmod: i32) -> Result<Option<CharLength>, String> {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L139
        if typmod >= VARHDRSZ {
            Ok(Some(CharLength(typmod - VARHDRSZ)))
        } else {
            Ok(None)
        }
    }

    fn into_typmod(&self) -> i32 {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L60-L65
        self.0 + VARHDRSZ
    }
}

impl fmt::Display for CharLength {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/varchar.c#L77
        write!(f, "({})", self.0)
    }
}

/// Constraints associated with [`Type::Interval`]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IntervalConstraints {
    /// The range of the interval.
    range: i32,
    /// The precision of the interval.
    precision: i32,
}

impl TypeConstraint for IntervalConstraints {
    fn from_typmod(typmod: i32) -> Result<Option<IntervalConstraints>, String> {
        if typmod < 0 {
            Ok(None)
        } else {
            // https://github.com/postgres/postgres/blob/27b77ecf9/src/include/utils/timestamp.h#L53-L54
            let range = typmod >> 16 & 0x7fff;
            let precision = typmod & 0xffff;
            if precision > MAX_INTERVAL_PRECISION {
                return Err(format!(
                    "exceeds maximum interval precision {MAX_INTERVAL_PRECISION}"
                ));
            }
            Ok(Some(IntervalConstraints { range, precision }))
        }
    }

    fn into_typmod(&self) -> i32 {
        (self.range << 16) | self.precision
    }
}

impl fmt::Display for IntervalConstraints {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/27b77ecf9/src/include/utils/timestamp.h#L52
        // TODO: handle output of range.
        write!(f, "({})", self.precision)
    }
}

/// A precision associated with [`Type::Time`] and [`Type::TimeTz`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TimePrecision(i32);

impl TypeConstraint for TimePrecision {
    fn from_typmod(typmod: i32) -> Result<Option<TimePrecision>, String> {
        if typmod > MAX_TIME_PRECISION {
            Err(format!(
                "exceeds maximum time precision {MAX_TIME_PRECISION}"
            ))
        } else if typmod >= 0 {
            Ok(Some(TimePrecision(typmod)))
        } else {
            Ok(None)
        }
    }

    fn into_typmod(&self) -> i32 {
        self.0
    }
}

impl fmt::Display for TimePrecision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/27b77ecf9/src/backend/utils/adt/date.c#L97
        write!(f, "({})", self.0)
    }
}

/// A precision associated with [`Type::Timestamp`] and [`Type::TimestampTz`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TimestampPrecision(i32);

impl TypeConstraint for TimestampPrecision {
    fn from_typmod(typmod: i32) -> Result<Option<TimestampPrecision>, String> {
        if typmod > MAX_TIMESTAMP_PRECISION {
            Err(format!(
                "exceeds maximum timestamp precision {MAX_TIMESTAMP_PRECISION}"
            ))
        } else if typmod >= 0 {
            Ok(Some(TimestampPrecision(typmod)))
        } else {
            Ok(None)
        }
    }

    fn into_typmod(&self) -> i32 {
        self.0
    }
}

impl fmt::Display for TimestampPrecision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/54bd1e43c/src/backend/utils/adt/timestamp.c#L131
        write!(f, "({})", self.0)
    }
}

/// Constraints on [`Type::Numeric`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NumericConstraints {
    /// The maximum precision.
    max_precision: i32,
    /// The maximum scale.
    max_scale: i32,
}

impl TypeConstraint for NumericConstraints {
    fn from_typmod(typmod: i32) -> Result<Option<NumericConstraints>, String> {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/numeric.c#L829-L862
        if typmod >= VARHDRSZ {
            Ok(Some(NumericConstraints {
                max_precision: ((typmod - VARHDRSZ) >> 16) & 0xffff,
                max_scale: (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024,
            }))
        } else {
            Ok(None)
        }
    }

    fn into_typmod(&self) -> i32 {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/numeric.c#L826
        ((self.max_precision << 16) | (self.max_scale & 0x7ff)) + VARHDRSZ
    }
}

impl fmt::Display for NumericConstraints {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // https://github.com/postgres/postgres/blob/52377bb81/src/backend/utils/adt/numeric.c#L1292-L1294
        write!(f, "({},{})", self.max_precision, self.max_scale)
    }
}

impl TryFrom<&Type> for ScalarType {
    type Error = TypeConversionError;

    fn try_from(typ: &Type) -> Result<ScalarType, TypeConversionError> {
        match typ {
            Type::Bool => Ok(ScalarType::Boolean),
            Type::Int2 => Ok(ScalarType::Int16),
            Type::Int4 => Ok(ScalarType::Int32),
            Type::Int8 => Ok(ScalarType::Int64),
            Type::VarChar { max_length } => Ok(ScalarType::String),
            Type::Oid => Ok(ScalarType::Oid),
            other => Err(TypeConversionError::UnsupportedType(other.clone())),
        }
    }
}

/// An error that can occur when converting a [`Type`] to a [`ScalarType`].
#[derive(Debug, Clone)]
pub enum TypeConversionError {
    /// The source type is unsupported as a `ScalarType`.
    UnsupportedType(Type),
    /// The source type contained an invalid max scale for a
    /// [`ScalarType::Numeric`].
    InvalidNumericMaxScale(InvalidNumericMaxScaleError),
    /// The source type contained an invalid constraint for a
    /// [`ScalarType::Numeric`].
    InvalidNumericConstraint(String),
    /// The source type contained an invalid length for a
    /// [`ScalarType::Char`].
    InvalidCharLength(InvalidCharLengthError),
    /// The source type contained an invalid max length for a
    /// [`ScalarType::VarChar`].
    InvalidVarCharMaxLength(InvalidVarCharMaxLengthError),
}

impl fmt::Display for TypeConversionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TypeConversionError::UnsupportedType(ty) => {
                write!(f, "{}", format!("type {:?} not supported", ty))
            }
            TypeConversionError::InvalidNumericMaxScale(e) => e.fmt(f),
            TypeConversionError::InvalidNumericConstraint(msg) => f.write_str(msg),
            TypeConversionError::InvalidCharLength(e) => e.fmt(f),
            TypeConversionError::InvalidVarCharMaxLength(e) => e.fmt(f),
        }
    }
}

impl Error for TypeConversionError {}

impl From<&ScalarType> for Type {
    fn from(typ: &ScalarType) -> Self {
        todo!()
    }
}
