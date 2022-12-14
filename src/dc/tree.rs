use crate::common::error::Result;
use crate::dc::{
    buf_frame::BufferFrameRef,
    buf_mgr::BufMgr,
    node::{InteriorNode, LeafNode, NodeType},
    page::{PageId, PAGE_ID_ROOT},
    MAX_KEY_SIZE, MAX_VALUE_SIZE,
};
use std::ops::{Deref, DerefMut};

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
        // get_and_pin will extend the file if the page does not exist.
        let root = buf_mgr.fix_page(PAGE_ID_ROOT).await?;
        // let root_node = SlotArray::<&[u8], PageId>::new(root);
        todo!()
        // load all interior node into buffer pool.
    }

    pub fn close() -> Result<()> {
        todo!()
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        assert!(key.len() <= MAX_KEY_SIZE);
        let frame = self.find_leaf(key).await?;
        self.find_value(key, frame)
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(key.len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        let frame = self.find_leaf(key).await?;
        self.put_value(key, value, frame)
    }

    async fn find_leaf(&self, key: &[u8]) -> Result<BufferFrameRef> {
        let mut page_id = PAGE_ID_ROOT;
        loop {
            let frame = self.buf_mgr.fix_page(page_id).await?;
            {
                let guard = frame.lock().unwrap();
                match guard.node_type() {
                    NodeType::Leaf => return Ok(frame.clone()),
                    NodeType::Interior => {
                        page_id = self.find_child(key, frame.clone())?;
                    }
                }
            }
        }
    }

    fn find_child(&self, key: &[u8], frame: BufferFrameRef) -> Result<PageId> {
        let mut guard = frame.lock().unwrap();
        let node = InteriorNode::from_frame(guard.deref_mut());
        node.get_child(key)
    }

    fn find_value(&self, key: &[u8], frame: BufferFrameRef) -> Result<Option<Vec<u8>>> {
        let mut guard = frame.lock().unwrap();
        let node = LeafNode::from_frame(guard.deref_mut());
        node.get(key).map(|opt_v| opt_v.map(|v| v.into()))
    }

    fn put_value(&self, key: &[u8], value: &[u8], frame: BufferFrameRef) -> Result<()> {
        let mut guard = frame.lock().unwrap();
        let mut node = LeafNode::from_frame(guard.deref_mut());
        node.put(key, value)
    }
}
