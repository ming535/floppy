use crate::common::error::Result;
use crate::dc::{
    buf_mgr::BufMgr, page::PAGE_ID_ROOT, tree_node::TreeNode, MAX_KEY_SIZE, MAX_VALUE_SIZE,
};

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
        todo!()
        // load all interior node into buffer pool.
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

    fn find_leaf(&self, key: &[u8]) -> Result<TreeNode<&[u8], &[u8]>> {
        todo!()
    }
}
