use crate::value::numeric::{InvalidNumericMaxScaleError, NUMERIC_DATUM_MAX_PRECISION};
use common::adt::char::InvalidCharLengthError;
use common::adt::varchar::{InvalidVarCharMaxLengthError, VarCharMaxLength};
use common::error::FloppyError;
use common::scalar::ScalarType;
use postgres_types;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;

/// Mirror of PostgreSQL's [`VARHDRSZ`] constant.
///
/// [`VARHDRSZ`]: https://github.com/postgres/postgres/blob/REL_14_0/src/include/c.h#L627
const VARHDRSZ: i32 = 4;

/// Mirror of PostgreSQL's [`MAX_INTERVAL_PRECISION`]
/// constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/datatype/timestamp.h#L54>
const MAX_INTERVAL_PRECISION: i32 = 6;

/// Mirror of PostgreSQL's [`MAX_TIMESTAMP_PRECISION`]
/// constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/datatype/timestamp.h#L53>
const MAX_TIMESTAMP_PRECISION: i32 = 6;

/// Mirror of PostgreSQL's [`MAX_TIME_PRECISION`] constant.
///
/// See: <https://github.com/postgres/postgres/blob/27b77ecf9/src/include/utils/date.h#L51>
const MAX_TIME_PRECISION: i32 = 6;

/// The type of a [`Value`](crate::Value).
///
/// The [`Display`](fmt::Display) representation of a type
/// is guaranteed to be valid PostgreSQL syntax that names
/// the type and any modifiers.
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
    /// A 2-byte unsigned integer. This does not exist in
    /// PostgreSQL.
    UInt2,
    /// A 4-byte unsigned integer. This does not exist in
    /// PostgreSQL.
    UInt4,
    /// An 8-byte unsigned integer. This does not exist in
    /// PostgreSQL.
    UInt8,
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
        /// If unspecified, the type represents a
        /// variable-length string.
        length: Option<CharLength>,
    },
    /// A variable-length string with an optional limit.
    VarChar {
        /// An optional maximum length to enforce, in
        /// characters.
        max_length: Option<CharLength>,
    },
    /// A time of day without a day.
    Time {
        /// An optional precision for the fractional digits
        /// in the second field.
        precision: Option<TimePrecision>,
    },
    /// A time with a time zone.
    TimeTz {
        /// An optional precision for the fractional digits
        /// in the second field.
        precision: Option<TimePrecision>,
    },
    /// A date and time, without a timezone.
    Timestamp {
        /// An optional precision for the fractional digits
        /// in the second field.
        precision: Option<TimestampPrecision>,
    },
    /// A date and time, with a timezone.
    TimestampTz {
        /// An optional precision for the fractional digits
        /// in the second field.
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

impl Type {
    pub(crate) fn inner(&self) -> &'static postgres_types::Type {
        match self {
            Type::Array(inner) => unreachable!(),
            Type::Bool => &postgres_types::Type::BOOL,
            Type::Bytea => &postgres_types::Type::BYTEA,
            Type::Char => &postgres_types::Type::CHAR,
            Type::Date => &postgres_types::Type::DATE,
            Type::Float4 => &postgres_types::Type::FLOAT4,
            Type::Float8 => &postgres_types::Type::FLOAT8,
            Type::Int2 => &postgres_types::Type::INT2,
            Type::Int4 => &postgres_types::Type::INT4,
            Type::Int8 => &postgres_types::Type::INT8,
            Type::UInt2 | Type::UInt4 | Type::UInt8 => unreachable!(),
            Type::Interval { .. } => &postgres_types::Type::INTERVAL,
            Type::Json => &postgres_types::Type::JSON,
            Type::Jsonb => &postgres_types::Type::JSONB,
            Type::List(inner) => unreachable!(),
            Type::Map { value_type } => unreachable!(),
            Type::Numeric { .. } => &postgres_types::Type::NUMERIC,
            Type::Oid => &postgres_types::Type::OID,
            Type::Record(_) => &postgres_types::Type::RECORD,
            Type::Text => &postgres_types::Type::TEXT,
            Type::BpChar { .. } => &postgres_types::Type::BPCHAR,
            Type::VarChar { .. } => &postgres_types::Type::VARCHAR,
            Type::Time { .. } => &postgres_types::Type::TIME,
            Type::TimeTz { .. } => &postgres_types::Type::TIMETZ,
            Type::Timestamp { .. } => &postgres_types::Type::TIMESTAMP,
            Type::TimestampTz { .. } => &postgres_types::Type::TIMESTAMPTZ,
            Type::Uuid => &postgres_types::Type::UUID,
            Type::RegProc => &postgres_types::Type::REGPROC,
            Type::RegType => &postgres_types::Type::REGTYPE,
            Type::RegClass => &postgres_types::Type::REGCLASS,
            Type::Int2Vector => unreachable!(),
        }
    }

    /// Returns the [OID] of this type.
    /// Object identifiers (OIDs) are used internally by
    /// PostgreSQL as primary keys for various system
    /// tables. Type oid represents an object
    /// identifier.
    ///
    /// [OID]: https://www.postgresql.org/docs/current/datatype-oid.html
    pub fn oid(&self) -> u32 {
        self.inner().oid()
    }

    /// Returns the constraint on the type, if any.
    pub fn constraint(&self) -> Option<&dyn TypeConstraint> {
        match self {
            Type::BpChar {
                length: Some(length),
            } => Some(length),
            Type::VarChar {
                max_length: Some(max_length),
            } => Some(max_length),
            Type::Numeric {
                constraints: Some(constraints),
            } => Some(constraints),
            Type::Interval {
                constraints: Some(constraints),
            } => Some(constraints),
            Type::Time {
                precision: Some(precision),
            } => Some(precision),
            Type::TimeTz {
                precision: Some(precision),
            } => Some(precision),
            Type::Timestamp {
                precision: Some(precision),
            } => Some(precision),
            Type::TimestampTz {
                precision: Some(precision),
            } => Some(precision),
            Type::Array(_)
            | Type::Bool
            | Type::Bytea
            | Type::BpChar { length: None }
            | Type::Char
            | Type::Date
            | Type::Float4
            | Type::Float8
            | Type::Int2
            | Type::Int4
            | Type::Int8
            | Type::UInt2
            | Type::UInt4
            | Type::UInt8
            | Type::Interval { constraints: None }
            | Type::Json
            | Type::Jsonb
            | Type::List(_)
            | Type::Map { .. }
            | Type::Numeric { constraints: None }
            | Type::Int2Vector
            | Type::Oid
            | Type::Record(_)
            | Type::RegClass
            | Type::RegProc
            | Type::RegType
            | Type::Text
            | Type::Time { precision: None }
            | Type::TimeTz { precision: None }
            | Type::Timestamp { precision: None }
            | Type::TimestampTz { precision: None }
            | Type::Uuid
            | Type::VarChar { max_length: None } => None,
        }
    }

    /// Returns the number of bytes in the binary
    /// representation of this type, or -1 if the type
    /// has a variable-length representation.
    pub fn typlen(&self) -> i16 {
        match self {
            Type::Array(inner) => -1,
            Type::Bool => 1,
            Type::Bytea => -1,
            Type::Char => 1,
            Type::Date => 4,
            Type::Float4 => 4,
            Type::Float8 => 8,
            Type::Int2 => 2,
            Type::Int4 => 4,
            Type::Int8 => 8,
            Type::UInt2 => 2,
            Type::UInt4 => 4,
            Type::UInt8 => 8,
            Type::Interval { .. } => 16,
            Type::Json => -1,
            Type::Jsonb => -1,
            Type::List(_) => -1,
            Type::Map { .. } => -1,
            Type::Numeric { .. } => -1,
            Type::Oid => 4,
            Type::Record(_) => -1,
            Type::Text => -1,
            Type::BpChar { .. } => -1,
            Type::VarChar { .. } => -1,
            Type::Time { .. } => 8,
            Type::TimeTz { .. } => 12,
            Type::Timestamp { .. } => 8,
            Type::TimestampTz { .. } => 12,
            Type::Uuid => 16,
            Type::RegProc => 4,
            Type::RegType => 4,
            Type::RegClass => 4,
            Type::Int2Vector => -1,
        }
    }

    /// Returns the packed type modifier ("typmod") for the
    /// type.
    ///
    /// The typmod is a 32-bit integer associated with the
    /// type that encodes optional constraints on the
    /// type. For example, the typmod on `Type::VarChar`
    /// encodes an optional constraint on the value's
    /// length. Most types are never associated with a
    /// typmod.
    ///
    /// Negative typmods indicate no constraint.
    pub fn typmod(&self) -> i32 {
        match self.constraint() {
            Some(constraint) => constraint.into_typmod(),
            None => -1,
        }
    }
}

impl TryFrom<&Type> for ScalarType {
    type Error = FloppyError;

    fn try_from(typ: &Type) -> Result<ScalarType, FloppyError> {
        match typ {
            Type::Bool => Ok(ScalarType::Boolean),
            Type::Int2 => Ok(ScalarType::Int16),
            Type::Int4 => Ok(ScalarType::Int32),
            Type::Int8 => Ok(ScalarType::Int64),
            Type::VarChar { max_length } => Ok(ScalarType::String),
            Type::Oid => Ok(ScalarType::Oid),
            other => Err(FloppyError::NotImplemented(format!(
                "Type {:?} is not implemented",
                other
            ))),
        }
    }
}

impl From<&ScalarType> for Type {
    fn from(typ: &ScalarType) -> Self {
        match typ {
            ScalarType::Boolean => Type::Bool,
            ScalarType::Int16 => Type::Int2,
            ScalarType::Int32 => Type::Int4,
            ScalarType::Int64 => Type::Int8,
            ScalarType::String => Type::Text,
            ScalarType::VarChar { max_length } => Type::VarChar {
                max_length: (*max_length).map(CharLength::from),
            },
            ScalarType::Oid => Type::Oid,
        }
    }
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

/// A length associated with [`Type::Char`] and
/// [`Type::VarChar`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CharLength(i32);

impl From<VarCharMaxLength> for CharLength {
    fn from(length: VarCharMaxLength) -> CharLength {
        // The `VarCharMaxLength` newtype wrapper ensures that the
        // inner `u32` is small enough to fit into an `i32`
        // with room for `VARHDRSZ`.
        CharLength(i32::try_from(length.into_u32()).unwrap())
    }
}

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

/// A precision associated with [`Type::Time`] and
/// [`Type::TimeTz`].
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

/// A precision associated with [`Type::Timestamp`] and
/// [`Type::TimestampTz`].
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

/// An error that can occur when converting a [`Type`] to a
/// [`ScalarType`].
#[derive(Debug, Clone)]
pub enum TypeConversionError {
    /// The source type is unsupported as a `ScalarType`.
    UnsupportedType(Type),
    /// The source type contained an invalid max scale for a
    /// [`ScalarType::Numeric`].
    InvalidNumericMaxScale(InvalidNumericMaxScaleError),
    /// The source type contained an invalid constraint for
    /// a [`ScalarType::Numeric`].
    InvalidNumericConstraint(String),
    /// The source type contained an invalid length for a
    /// [`ScalarType::Char`].
    InvalidCharLength(InvalidCharLengthError),
    /// The source type contained an invalid max length for
    /// a [`ScalarType::VarChar`].
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
