mod catalog;
mod common;
mod session;
mod sql;
mod storage;
mod test_util;

pub use common::error::Result;
use session::Session;

pub fn open() -> Result<Session> {
    Session::open()
}
