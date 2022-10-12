use std::error::Error;
use std::fmt;

/// The number of internal decimal units in a [`Numeric`] value.
pub const NUMERIC_DATUM_WIDTH: u8 = 13;

/// The maximum number of digits expressable in a [`Numeric`] value.
pub const NUMERIC_DATUM_MAX_PRECISION: u8 = NUMERIC_DATUM_WIDTH * 3;

/// The error returned when constructing a [`NumericMaxScale`] from an invalid
/// value.
#[derive(Debug, Clone)]
pub struct InvalidNumericMaxScaleError;

impl fmt::Display for InvalidNumericMaxScaleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "scale for type numeric must be between 0 and {}",
            NUMERIC_DATUM_MAX_PRECISION
        )
    }
}

impl Error for InvalidNumericMaxScaleError {}
