use crate::common::error::Result;
use std::path::Path;

pub struct Tree {}

impl Tree {
    /// Open a tree from the given path.
    /// The root of the tree is stored in Page 1.
    /// All index pages are read into buffer pool.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        todo!()
    }

    pub fn close() -> Result<()> {
        todo!()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }
}
