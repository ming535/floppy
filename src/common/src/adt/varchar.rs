use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

// https://github.com/postgres/postgres/blob/REL_14_0/src/include/access/htup_details.h#L577-L584
pub const MAX_MAX_LENGTH: u32 = 10_485_760;

/// The `max_length` of a [`ScalarType::VarChar`].
///
/// This newtype wrapper ensures that the length is within the valid range.
///
/// [`ScalarType::VarChar`]: crate::ScalarType::VarChar
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct VarCharMaxLength(pub(crate) u32);

impl VarCharMaxLength {
    /// Consumes the newtype wrapper, returning the inner `u32`.
    pub fn into_u32(self) -> u32 {
        self.0
    }
}

impl TryFrom<i64> for VarCharMaxLength {
    type Error = InvalidVarCharMaxLengthError;

    fn try_from(max_length: i64) -> Result<Self, Self::Error> {
        match u32::try_from(max_length) {
            Ok(max_length) if max_length > 0 && max_length < MAX_MAX_LENGTH => {
                Ok(VarCharMaxLength(max_length))
            }
            _ => Err(InvalidVarCharMaxLengthError),
        }
    }
}

/// The error returned when constructing a [`VarCharMaxLength`] from an invalid
/// value.
#[derive(Debug, Clone)]
pub struct InvalidVarCharMaxLengthError;

impl fmt::Display for InvalidVarCharMaxLengthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "length for type character varying must be between 1 and {}",
            MAX_MAX_LENGTH
        )
    }
}

impl Error for InvalidVarCharMaxLengthError {}
