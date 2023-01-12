#![feature(
    type_alias_impl_trait,
    io_error_more,
    anonymous_lifetime_in_impl_trait,
    int_roundings
)]
// todo fix this
#![allow(dead_code)]

mod catalog;
mod common;
// mod dc;
mod dc2;
mod env;
mod session;
mod sql;
mod storage;
mod tc;
mod test_util;

pub use common::error::Result;
use session::Session;

pub fn open() -> Result<Session> {
    Session::open()
}
