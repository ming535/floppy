// Copyright Materialize, Inc. and contributors. All rights
// reserved.
//
// Use of this software is governed by the Business Source
// License included in the LICENSE file.
//
// As of the Change Date specified in that file, in
// accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version
// 2.0.

//! Representation of and serialization for PostgreSQL
//! datums.
//!
//! This crate exports a [`Value`] type that maps directly
//! to a PostgreSQL datum. These values can be serialized
//! using either the text or binary encoding format; see the
//! [`Format`] type for details.
//!
//! `Value`s are easily converted to and from
//! [`common::Datum`]s. See, for example, the
//! [`values_from_row`] function.

mod format;
mod types;
mod value;

pub use format::Format;
pub use types::{Type, TypeConversionError};
