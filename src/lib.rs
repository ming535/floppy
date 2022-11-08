use common::error::Result;

pub struct Db {}

pub fn open() -> Result<Db> {
    Ok(Db {})
}
