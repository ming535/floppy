use crate::common::error::Result;
use crate::dc::buf_mgr::BufMgr;
use crate::dc::page::PAGE_ID_ROOT;
use crate::dc::{MAX_KEY_SIZE, MAX_VALUE_SIZE};
use crate::env::Env;
use std::path::Path;

pub struct Tree {
    buf_mgr: BufMgr,
}

impl Tree {
    /// Open a tree from the given path.
    /// The root of the tree is stored in Page 1.
    /// All interior pages are read into buffer pool.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let buf_mgr = BufMgr::open(path, 1000).await?;
        let page_frame = buf_mgr.get_and_pin(PAGE_ID_ROOT)?;
        todo!()
    }

    pub fn close() -> Result<()> {
        todo!()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        assert!(key.len() <= MAX_KEY_SIZE);
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(key.len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        todo!()
    }
}
