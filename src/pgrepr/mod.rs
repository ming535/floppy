//! Representation of and serialization for PostgreSQL datums.
//!
//! This crate exports a [`Value`] type that maps directly to a PostgreSQL
//! datum. These values can be serialized using either the text or binary
//! encoding format; see the [`Format`] type for details.
//!
//! `Value`s are easily converted to and from [`mz_repr::Datum`]s. See, for
//! example, the [`values_from_row`] function.

mod format;

pub use format::Format;
