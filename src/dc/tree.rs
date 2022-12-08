use crate::common::error::Result;
use crate::dc::{
    buf_frame::BufferFrame,
    buf_mgr::BufMgr,
    page::{PageId, PAGE_ID_ROOT},
    MAX_KEY_SIZE, MAX_VALUE_SIZE,
};

use crate::dc::tree_node::{PAGE_TYPE_INTERIOR, PAGE_TYPE_LEAF, PAGE_TYPE_ROOT};
use crate::env::Env;
use std::path::Path;

pub struct Tree<E: Env> {
    buf_mgr: BufMgr<E>,
}

impl<E> Tree<E>
where
    E: Env,
{
    /// Open a tree from the given path.
    /// The root of the tree is stored in Page 1.
    /// All interior pages are read into buffer pool.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let buf_mgr = BufMgr::<E>::open(path, 1000).await?;
        // get_and_pin will extend the file if the page does not exist.
        let root = buf_mgr.get_and_pin(PAGE_ID_ROOT)?;
        // let root_node = SlotArray::<&[u8], PageId>::new(root);
        todo!()
        // load all interior node into buffer pool.
    }

    pub fn close() -> Result<()> {
        todo!()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        assert!(key.len() <= MAX_KEY_SIZE);
        let frame = self.find_leaf(key)?;
        self.find_value(key, frame)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(key.len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        let frame = self.find_leaf(key)?;
        self.put_value(key, value, frame)
    }

    fn find_leaf(&self, key: &[u8]) -> Result<&BufferFrame> {
        let mut page_id = PAGE_ID_ROOT;
        loop {
            let frame = self.buf_mgr.get_and_pin(page_id)?;
            if frame.get_page_type() == PAGE_TYPE_LEAF {
                return Ok(frame);
            } else {
                page_id = self.find_child(key, frame)?;
            }
        }
    }

    fn find_child(&self, key: &[u8], frame: &BufferFrame) -> Result<PageId> {
        todo!()
    }

    fn find_value(&self, key: &[u8], frame: &BufferFrame) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    fn put_value(&self, key: &[u8], value: &[u8], frame: &BufferFrame) -> Result<()> {
        todo!()
    }
}
