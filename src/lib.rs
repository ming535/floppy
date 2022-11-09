pub use common::error::Result;
use session::Session;

pub fn open() -> Result<Session> {
    Session::open()
}
