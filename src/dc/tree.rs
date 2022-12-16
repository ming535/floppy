use crate::common::error::{DCError, FloppyError, Result};
use crate::dc::{
    buf_frame::{BufferFrame, BufferFrameGuard},
    buf_mgr::BufMgr,
    node::{InteriorNode, LeafNode, NodeType},
    page::{PageId, PAGE_ID_ROOT},
    MAX_KEY_SIZE, MAX_VALUE_SIZE,
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
    pub async fn open<P: AsRef<Path>>(path: P, env: E) -> Result<Self> {
        let buf_mgr = BufMgr::open(env, path, 1000).await?;
        Self::init_index(&buf_mgr).await?;
        Ok(Self { buf_mgr })
    }

    pub fn close() -> Result<()> {
        todo!()
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        assert!(key.len() <= MAX_KEY_SIZE);
        let leaf_guard = self.find_leaf(key).await?;
        self.find_value(key, leaf_guard)
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(key.len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        let leaf_guard = self.find_leaf(key).await?;
        self.put_value(key, value, leaf_guard)
    }

    /// init root node if not exists
    async fn init_index(buf_mgr: &BufMgr<E>) -> Result<()> {
        match buf_mgr.fix_page(PAGE_ID_ROOT).await {
            Err(FloppyError::DC(DCError::PageNotFound(_))) => {
                let guard = buf_mgr.alloc_page().await?;
                assert_eq!(guard.page_id(), PAGE_ID_ROOT);
                Ok(())
            }
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
        // todo read all interior pages into buffer pool
    }

    async fn find_leaf(&self, key: &[u8]) -> Result<BufferFrameGuard> {
        let mut page_id = PAGE_ID_ROOT;
        let mut guard = self.buf_mgr.fix_page(page_id).await?;
        loop {
            match guard.node_type() {
                NodeType::Leaf => return Ok(guard),
                NodeType::Interior => {
                    page_id = self.find_child(key, &mut guard)?;
                    let child_guard = self.buf_mgr.fix_page(page_id).await?;
                    // this will drop the parent node's guard while we hold the child node's guard.
                    guard = child_guard;
                }
            }
        }
    }

    fn find_child(&self, key: &[u8], parent_guard: &mut BufferFrameGuard) -> Result<PageId> {
        let node = InteriorNode::from_data(parent_guard.payload_mut());
        node.get_child(key)
    }

    fn find_value(&self, key: &[u8], mut guard: BufferFrameGuard) -> Result<Option<Vec<u8>>> {
        let node = LeafNode::from_data(guard.payload_mut());
        node.get(key).map(|opt_v| opt_v.map(|v| v.into()))
    }

    fn put_value(&self, key: &[u8], value: &[u8], mut guard: BufferFrameGuard) -> Result<()> {
        let mut node = LeafNode::from_data(guard.payload_mut());
        node.put(key, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::sim::{SimEnv, SimPath};

    #[tokio::test]
    async fn test_simple() -> Result<()> {
        let env = SimEnv;
        let tree = Tree::open(SimPath, env).await?;
        tree.put(b"1", b"1").await?;
        let v = tree.get(b"1").await?;
        assert_eq!(v, Some(b"1".to_vec()));
        Ok(())
    }
}
