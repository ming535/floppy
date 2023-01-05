use crate::common::{
    error::{DCError, FloppyError, Result},
    ivec::IVec,
};
use crate::dc::{
    buf_frame::{BufferFrame, BufferFrameGuard},
    buf_mgr::BufMgr,
    node::{InteriorNode, LeafNode, NodeType, NodeValue, TreeNode},
    page::{PageId, PAGE_ID_ROOT},
    MAX_KEY_SIZE, MAX_VALUE_SIZE,
};
use crate::env::Env;
use std::{cmp::Ordering, path::Path};

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

    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        assert!(key.as_ref().len() <= MAX_KEY_SIZE);
        let mut guard_chain =
            self.find_leaf(key.as_ref(), AccessMode::Read).await?;
        assert_eq!(guard_chain.len(), 1);
        self.find_value(key.as_ref(), &mut guard_chain[0])
    }

    pub async fn insert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<IVec>,
    {
        let value = value.into();
        assert!(key.as_ref().len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        let guard_chain =
            self.find_leaf(key.as_ref(), AccessMode::Insert).await?;
        self.insert_value(key.as_ref(), value, guard_chain).await
    }

    /// init root node if not exists
    async fn init_index(buf_mgr: &BufMgr<E>) -> Result<()> {
        match buf_mgr.fix_page(PAGE_ID_ROOT).await {
            Err(FloppyError::DC(DCError::PageNotFound(_))) => {
                let guard = buf_mgr.alloc_page().await?;
                guard.page_ptr().set_node_type(NodeType::Leaf);
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
    /// For [`AccessMode::Insert`] or [`AccessMode::Delete`], we repeat these
    /// steps until we reach the leaf node:
    /// 1. Acquire X latch on parent
    /// 2. Acquire X latch on child
    /// 3. Check if child is safe (can be inserted or deleted without
    /// split/merge) 3.1 If it is safe, unlatch all ancestor's latch
    /// 3.2 If it is not safe, go deeper in the tree
    /// After we reach the leaf node, we may have a chain of X latches on the
    /// path.
    async fn find_leaf(
        &self,
        key: &[u8],
        mode: AccessMode,
    ) -> Result<Vec<BufferFrameGuard>> {
        let mut page_id = PAGE_ID_ROOT;
        let mut guard_chain = vec![];
        let mut guard = self.buf_mgr.fix_page(page_id).await?;
        loop {
            match guard.page_ptr().node_type() {
                NodeType::Leaf => {
                    guard_chain.push(guard);
                    return Ok(guard_chain);
                }
                NodeType::Interior => {
                    page_id = self.find_child(key, &mut guard)?;
                    let child_guard = self.buf_mgr.fix_page(page_id).await?;
                    // add parent to the guard chain if SMO might happen.
                    let child_node =
                        InteriorNode::from_page(child_guard.page_ptr())?;
                    if (mode == AccessMode::Insert
                        && child_node.slot_array().will_overfull(key, page_id))
                        || (mode == AccessMode::Delete
                            && child_node.slot_array().will_underfull())
                    {
                        let parent_guard = guard;
                        guard_chain.push(parent_guard);
                        guard = child_guard;
                    } else {
                        //
                        // the child is safe, we can release all latches on
                        // ancestors
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

    fn find_child(
        &self,
        key: &[u8],
        parent_guard: &mut BufferFrameGuard,
    ) -> Result<PageId> {
        let node = InteriorNode::from_page(parent_guard.page_ptr())?;
        node.get(key).map(|v| v.unwrap())
    }

    fn find_value(
        &self,
        key: &[u8],
        guard: &mut BufferFrameGuard,
    ) -> Result<Option<IVec>> {
        let node = LeafNode::from_page(guard.page_ptr())?;
        node.get(key).map(|opt_v| opt_v.map(|v| v.into()))
    }

    async fn insert_value(
        &self,
        key: &[u8],
        value: IVec,
        mut guard_chain: Vec<BufferFrameGuard>,
    ) -> Result<()> {
        let chain_len = guard_chain.len();
        assert!(chain_len >= 1);
        let leaf_guard = &mut guard_chain[chain_len - 1];
        let node = LeafNode::from_page(leaf_guard.page_ptr())?;
        if node.slot_array().will_overfull(key, value.clone()) {
            self.split(key, value, guard_chain.as_mut_slice()).await
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
    ///    Copy all old sorted array A and new key value into a new sorted array
    /// N. 4. Split the new sorted array into two sorted array iterator:
    /// `Iter-left` and `Iter-right`. 3. Replace N's content with
    /// `Iter-left`. 4. Construct a new node with `Iter-right`.
    ///
    /// After leaf node split, we post a new index to interior node.
    ///
    /// Interior Node split happens when we want to add a index entry for split
    /// key S with left children P-left, and right children P-right (S,
    /// P-left, P-right). Interior Node `N` split:
    /// 1. Get the rank of `S`.
    /// 2. Construct two range iterator:
    ///   - `Iter-left` with range [0..rank).
    ///   - `Iter-right` with range [range, ..).
    /// 3. Copy elements of `Iter-left` and (S, P-left, P-right) into a new
    /// sorted array `Array-left`. 5. Copy elements of `Iter-right` into a
    /// new sorted array `Array-right`. 4. Replace `N`'s content with sorted
    /// array `Array-left`. 5. Construct a new node `N-right` with sorted
    /// array `Array-right`. 6. Post a new index entry (S, N, N-right) into
    /// parent node.
    ///
    /// If the leaf node is root:
    /// 1. Construct `Iter-left` and `Iter-right` as before.
    /// 2. Construct a new node `N-left` with `Iter-left`.
    /// 3. Construct a new node `N-right` with `Iter-right`.
    /// 4. Replace the current node with a new interior node with (S, N-left,
    /// N-right) where S is the split key.
    ///
    /// If the interior node is root:
    /// 1. Construct `Iter-left` and `Iter-right` as before.
    /// 2. Construct a new node `N-left` with `Iter-left` and (S, P-left,
    /// P-right). 3. Construct a new node `N-right` with `Iter-right`.
    async fn split(
        &self,
        key: &[u8],
        value: IVec,
        guard_chain: &mut [BufferFrameGuard],
    ) -> Result<()> {
        guard_chain.reverse();

        assert!(guard_chain.len() > 0);

        let leaf_guard = &mut guard_chain[0];

        if leaf_guard.page_id() == PAGE_ID_ROOT {
            let new_left = self.buf_mgr.alloc_page().await?;
            let new_right = self.buf_mgr.alloc_page().await?;
            return self
                .split_root::<IVec, LeafNode>(
                    leaf_guard, &new_left, &new_right, key, value,
                )
                .await;
        }

        let mut new_page = self.buf_mgr.alloc_page().await?;
        self.split_node::<IVec, LeafNode>(leaf_guard, &new_page, key, value)
            .await?;
        let new_node = LeafNode::from_page(new_page.page_ptr())?;
        let mut split_key = new_node.min_key();

        let parents = &mut guard_chain[1..];
        for guard in parents.iter_mut() {
            let node = InteriorNode::from_page(guard.page_ptr())?;
            if node
                .slot_array()
                .will_overfull(&split_key, new_page.page_id())
            {
                if guard.page_id() == PAGE_ID_ROOT {
                    let new_left = self.buf_mgr.alloc_page().await?;
                    let new_right = self.buf_mgr.alloc_page().await?;
                    return self
                        .split_root::<PageId, InteriorNode>(
                            guard,
                            &new_left,
                            &new_right,
                            &split_key,
                            new_page.page_id(),
                        )
                        .await;
                }
                new_page = self.buf_mgr.alloc_page().await?;
                self.split_node::<PageId, InteriorNode>(
                    guard,
                    &new_page,
                    &split_key,
                    new_page.page_id(),
                )
                .await?;
                let new_node = InteriorNode::from_page(new_page.page_ptr())?;
                split_key = new_node.set_inf_min();
            } else {
                node.insert(&split_key, new_page.page_id())?;
                break;
            }
        }
        Ok(())
    }

    async fn split_node<'a, V, Node>(
        &self,
        guard: &'a BufferFrameGuard,
        new_page: &'a BufferFrameGuard,
        key: &'a [u8],
        value: V,
    ) -> Result<()>
    where
        V: NodeValue,
        Node: TreeNode<'a, &'a [u8], V>,
    {
        let node = Node::from_page(guard.page_ptr())?;
        let (split_key, left_iter, right_iter) = node.slot_array().split_half();

        let right_node = Node::from_page(new_page.page_ptr())?;
        right_node.slot_array().with_iter(right_iter)?;

        let cmp = key.cmp(split_key);
        if cmp == Ordering::Less {
            node.insert(key, value)?;
        } else if cmp == Ordering::Greater {
            right_node.insert(key, value)?;
        } else {
            return Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "key already exists {:?}",
                key
            ))));
        }

        Ok(())
    }

    async fn split_root<'a, V, Node>(
        &self,
        guard: &'a BufferFrameGuard,
        new_left_page: &'a BufferFrameGuard,
        new_right_page: &'a BufferFrameGuard,
        key: &'a [u8],
        value: V,
    ) -> Result<()>
    where
        V: NodeValue,
        Node: TreeNode<'a, &'a [u8], V>,
    {
        let node = Node::from_page(guard.page_ptr())?;
        let (split_key, left_iter, right_iter) = node.slot_array().split_half();

        let new_left_node = Node::from_page(new_left_page.page_ptr())?;
        new_left_node.slot_array().with_iter(left_iter)?;

        let new_right_node = Node::from_page(new_right_page.page_ptr())?;
        new_right_node.slot_array().with_iter(right_iter)?;

        // todo
        // set inf min

        let cmp = key.cmp(split_key);
        if cmp == Ordering::Less {
            new_left_node.insert(key, value)?;
        } else if cmp == Ordering::Greater {
            new_right_node.insert(key, value)?;
        } else {
            return Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "key already exists {:?}",
                key
            ))));
        }

        let root = InteriorNode::from_page(guard.page_ptr())?;
        root.init(split_key, new_left_page.page_id(), new_right_page.page_id())
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
    use crate::env::sim::{SimEnv, SIM_PATH};

    #[tokio::test]
    async fn test_tree_simple() -> Result<()> {
        let env = SimEnv;
        let tree = Tree::open(SIM_PATH, env).await?;
        tree.insert(b"1", b"1").await?;
        let v = tree.get(b"1").await?;
        assert_eq!(v, Some(b"1".into()));
        Ok(())
    }
}
