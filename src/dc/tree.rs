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
        let mut guard_chain = self.find_leaf(key, AccessMode::Read).await?;
        assert_eq!(guard_chain.len(), 1);
        self.find_value(key, &mut guard_chain[0])
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(key.len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        let mut guard_chain = self.find_leaf(key, AccessMode::Insert).await?;
        self.insert_value(key, value, guard_chain)
    }

    /// init root node if not exists
    async fn init_index(buf_mgr: &BufMgr<E>) -> Result<()> {
        match buf_mgr.fix_page(PAGE_ID_ROOT).await {
            Err(FloppyError::DC(DCError::PageNotFound(_))) => {
                let mut guard = buf_mgr.alloc_page().await?;
                guard.set_node_type(NodeType::Leaf);
                assert_eq!(guard.page_id(), PAGE_ID_ROOT);
                Ok(())
            }
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
        // todo read all interior pages into buffer pool
    }

    /// Find the leaf node that contains the given key using latch coupling.
    /// For [`AccessMode::Read`], we repeat 1 ~ 3 until we reach the leaf node:
    /// 1. Acquire R latch on parent
    /// 2. Acquire R latch on child
    /// 3. Unlatch parent
    /// After we reach the leaf node, we only have a R latch on the leaf node.
    ///
    /// For [`AccessMode::Insert`] or [`AccessMode::Delete`], we repeat these steps
    /// until we reach the leaf node:
    /// 1. Acquire X latch on parent
    /// 2. Acquire X latch on child
    /// 3. Check if child is safe (can be inserted or deleted without split/merge)
    /// 3.1 If it is safe, unlatch all ancestor's latch
    /// 3.2 If it is not safe, go deeper in the tree
    /// After we reach the leaf node, we may have a chain of X latches on the path.
    async fn find_leaf(&self, key: &[u8], mode: AccessMode) -> Result<Vec<BufferFrameGuard>> {
        let mut page_id = PAGE_ID_ROOT;
        let mut guard_chain = vec![];
        let mut guard = self.buf_mgr.fix_page(page_id).await?;
        loop {
            match guard.node_type() {
                NodeType::Leaf => {
                    guard_chain.push(guard);
                    return Ok(guard_chain);
                }
                NodeType::Interior => {
                    page_id = self.find_child(key, &mut guard)?;
                    let mut child_guard = self.buf_mgr.fix_page(page_id).await?;
                    // add parent to the guard chain if SMO might happen.
                    let child_node = InteriorNode::from_data(child_guard.payload_mut());
                    if (mode == AccessMode::Insert && child_node.may_split())
                        || (mode == AccessMode::Delete && child_node.may_merge())
                    {
                        let parent_guard = guard;
                        guard_chain.push(parent_guard);
                        guard = child_guard;
                    } else {
                        //
                        // the child is safe, we can release all latches on ancestors
                        //
                        // 1. drop parent's guard to release its latch
                        guard = child_guard;
                        // 2. drop all ancestor's guard to release their latches
                        guard_chain.clear();
                    }
                }
            }
        }
    }

    fn find_child(&self, key: &[u8], parent_guard: &mut BufferFrameGuard) -> Result<PageId> {
        let node = InteriorNode::from_data(parent_guard.payload_mut());
        node.get_child(key)
    }

    fn find_value(&self, key: &[u8], guard: &mut BufferFrameGuard) -> Result<Option<Vec<u8>>> {
        let node = LeafNode::from_data(guard.payload_mut());
        node.get(key).map(|opt_v| opt_v.map(|v| v.into()))
    }

    fn insert_value(
        &self,
        key: &[u8],
        value: &[u8],
        mut guard_chain: Vec<BufferFrameGuard>,
    ) -> Result<()> {
        let chain_len = guard_chain.len();
        assert!(chain_len >= 1);
        let leaf_guard = &mut guard_chain[chain_len - 1];
        let mut node = LeafNode::from_data(leaf_guard.payload_mut());
        if node.will_overfull(key, value) {
            self.split(key, value, guard_chain)
        } else {
            // drop parent guards to release their latches
            node.insert(key, value)
        }
    }

    /// Leaf Node `N` split:
    /// 1. Construct a iterator `Iter`.
    /// 2. Copy elements of `Iter` into a new sorted array `Array`.
    ///    Make sure `Array` has enough space for the new key value.
    /// 3. Insert new key value pair into `Array`.
    ///    Copy all old sorted array A and new key value into a new sorted array N.
    /// 4. Split the new sorted array into two sorted array iterator: `Iter-left` and `Iter-right`.
    /// 3. Replace N's content with `Iter-left`.
    /// 4. Construct a new node with `Iter-right`.
    ///
    /// After leaf node split, we post a new index to interior node.
    ///
    /// Interior Node split happens when we want to add a index entry for split key S with
    /// left children P-left, and right children P-right (S, P-left, P-right).
    /// Interior Node `N` split:
    /// 1. Get the rank of `S`.
    /// 2. Construct two range iterator:
    ///   - `Iter-left` with range [0..rank).
    ///   - `Iter-right` with range [range, ..).
    /// 3. Copy elements of `Iter-left` and (S, P-left, P-right) into a new sorted array `Array-left`.
    /// 5. Copy elements of `Iter-right` into a new sorted array `Array-right`.
    /// 4. Replace `N`'s content with sorted array `Array-left`.
    /// 5. Construct a new node `N-right` with sorted array `Array-right`.
    /// 6. Post a new index entry (S, N, N-right) into parent node.
    ///
    /// If the leaf node is root:
    /// 1. Construct `Iter-left` and `Iter-right` as before.
    /// 2. Construct a new node `N-left` with `Iter-left`.
    /// 3. Construct a new node `N-right` with `Iter-right`.
    /// 4. Replace the current node with a new interior node with (S, N-left, N-right) where S is the split key.
    ///
    /// If the interior node is root:
    /// 1. Construct `Iter-left` and `Iter-right` as before.
    /// 2. Construct a new node `N-left` with `Iter-left` and (S, P-left, P-right).
    /// 3. Construct a new node `N-right` with `Iter-right`.
    fn split(&self, key: &[u8], value: &[u8], guard_chain: Vec<BufferFrameGuard>) -> Result<()> {
        Ok(())
    }
}

#[derive(Eq, PartialEq)]
enum AccessMode {
    Read,
    Insert,
    Delete,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::sim::{SimEnv, SimPath};

    #[tokio::test]
    async fn test_simple() -> Result<()> {
        let env = SimEnv;
        let tree = Tree::open(SimPath, env).await?;
        tree.insert(b"1", b"1").await?;
        let v = tree.get(b"1").await?;
        assert_eq!(v, Some(b"1".to_vec()));
        Ok(())
    }
}
