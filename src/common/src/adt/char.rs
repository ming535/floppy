use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

// https://github.com/postgres/postgres/blob/REL_14_0/src/include/access/htup_details.h#L577-L584
const MAX_LENGTH: u32 = 10_485_760;

/// The `length` of a [`ScalarType::Char`].
///
/// This newtype wrapper ensures that the length is within the valid range.
///
/// [`ScalarType::Char`]: crate::ScalarType::Char
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct CharLength(pub(crate) u32);

impl CharLength {
    /// A length of one.
    pub const ONE: CharLength = CharLength(1);

    /// Consumes the newtype wrapper, returning the inner `u32`.
    pub fn into_u32(self) -> u32 {
        self.0
    }
}

impl TryFrom<i64> for CharLength {
    type Error = InvalidCharLengthError;

    fn try_from(length: i64) -> Result<Self, Self::Error> {
        match u32::try_from(length) {
            Ok(length) if length > 0 && length < MAX_LENGTH => Ok(CharLength(length)),
            _ => Err(InvalidCharLengthError),
        }
    }
}

/// The error returned when constructing a [`CharLength`] from an invalid value.
#[derive(Debug, Clone)]
pub struct InvalidCharLengthError;

impl fmt::Display for InvalidCharLengthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "length for type character must be between 1 and {}",
            MAX_LENGTH
        )
    }
}

impl Error for InvalidCharLengthError {}
