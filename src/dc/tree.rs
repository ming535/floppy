use crate::common::{
    error::{DCError, FloppyError, Result},
    ivec::IVec,
};

use crate::dc::page::{PagePtr, PAGE_SIZE};
use crate::dc::slot_array::SlotArray;
use crate::dc::{
    buf_frame::BufferFrameGuard,
    buf_mgr::BufMgr,
    codec::Codec,
    node::{InteriorNode, LeafNode, NodeValue, TreeNode},
    page::{
        PageId,
        PageType::{TreeNodeInterior, TreeNodeLeaf},
        PAGE_ID_ROOT,
    },
    slot_array::Record,
    MAX_KEY_SIZE, MAX_VALUE_SIZE,
};
use crate::env::Env;
use std::{cmp::Ordering, path::Path};

pub(crate) struct Tree<E: Env> {
    buf_mgr: BufMgr<E>,
    options: TreeOptions,
}

impl<E> Tree<E>
where
    E: Env,
{
    /// Open a tree from the given path.
    /// The root of the tree is stored in Page 1.
    /// All interior pages are read into buffer pool.
    pub async fn open<P: AsRef<Path>>(
        path: P,
        env: E,
        options: TreeOptions,
    ) -> Result<Self> {
        let buf_mgr = BufMgr::open(env, path, 1000).await?;
        Self::init_index(&buf_mgr).await?;
        Ok(Self { buf_mgr, options })
    }

    pub fn close() -> Result<()> {
        todo!()
    }

    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        assert!(key.as_ref().len() <= MAX_KEY_SIZE);
        let mut guard_stack =
            self.find_leaf(key.as_ref(), AccessMode::Read).await?;
        let leaf_guard = guard_stack
            .pop()
            .ok_or(FloppyError::Internal("guard_stack empty".to_string()))?;
        self.find_value(key.as_ref(), &leaf_guard)
    }

    pub async fn insert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<IVec>,
    {
        let value = value.into();
        assert!(key.as_ref().len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        println!("--- insert key: {:?} ---", key.as_ref());
        let record = Record {
            flag: 0,
            key: key.as_ref(),
            value: value.clone(),
        };

        let guard_stack = self
            .find_leaf(key.as_ref(), AccessMode::Insert(record.encode_size()))
            .await?;
        self.insert_value(key.as_ref(), value, guard_stack).await
    }

    /// init root node if not exists
    async fn init_index(buf_mgr: &BufMgr<E>) -> Result<()> {
        match buf_mgr.fix_page(PAGE_ID_ROOT).await {
            Err(FloppyError::DC(DCError::PageNotFound(_))) => {
                let guard = buf_mgr.alloc_page_with_type(TreeNodeLeaf).await?;
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
        let mut guard_stack = vec![];
        let mut guard = self.buf_mgr.fix_page(page_id).await?;
        loop {
            match guard.page_ptr().page_type() {
                TreeNodeLeaf => {
                    guard_stack.push(guard);
                    return Ok(guard_stack);
                }
                TreeNodeInterior => {
                    page_id = self.find_child(key, &guard)?;
                    let child_guard = self.buf_mgr.fix_page(page_id).await?;
                    let child_type = child_guard.page_ptr().page_type();
                    match child_type {
                        TreeNodeLeaf => {
                            let child_node =
                                LeafNode::from_page(child_guard.page_ptr())?;
                            if self.child_is_safe(mode, child_node) {
                                guard_stack.push(child_guard);
                            } else {
                                guard_stack.push(guard);
                                guard_stack.push(child_guard);
                            }
                            return Ok(guard_stack);
                        }
                        TreeNodeInterior => {
                            // add parent to the guard chain if SMO might
                            // happen.
                            let child_node = InteriorNode::from_page(
                                child_guard.page_ptr(),
                            )?;
                            if self.child_is_safe(mode, child_node) {
                                //
                                // the child is safe, we can release all latches
                                // on ancestors
                                //
                                // 1. drop parent's guard to release its latch
                                guard = child_guard;
                                // 2. drop all ancestor's guard to release their
                                // latches
                                guard_stack.clear();
                            } else {
                                guard_stack.push(guard);
                                guard = child_guard;
                            }
                        }
                    }
                }
            }
        }
    }

    fn find_child(
        &self,
        key: &[u8],
        parent_guard: &BufferFrameGuard,
    ) -> Result<PageId> {
        let node = InteriorNode::from_page(parent_guard.page_ptr())?;
        node.get(key)?.ok_or(FloppyError::Internal(format!(
            "child page is none, key: {key:?}"
        )))
    }

    fn find_value(
        &self,
        key: &[u8],
        guard: &BufferFrameGuard,
    ) -> Result<Option<IVec>> {
        let node = LeafNode::from_page(guard.page_ptr())?;
        node.get(key)
    }

    fn child_is_safe<'a, V, Node>(&self, mode: AccessMode, child: Node) -> bool
    where
        V: NodeValue,
        Node: TreeNode<'a, &'a [u8], V>,
    {
        match mode {
            AccessMode::Insert(record_size) => !child
                .slot_array()
                .will_overfull(record_size, self.options.fanout),
            AccessMode::Delete => !child.slot_array().will_underfull(),
            AccessMode::Read => true,
        }
    }

    async fn insert_value(
        &self,
        key: &[u8],
        value: IVec,
        mut guard_stack: Vec<BufferFrameGuard>,
    ) -> Result<()> {
        let stack_len = guard_stack.len();
        assert!(stack_len >= 1);
        let leaf_guard = &mut guard_stack[stack_len - 1];
        let node = LeafNode::from_page(leaf_guard.page_ptr())?;
        let record = Record {
            flag: 0,
            key,
            value: value.clone(),
        };
        if node
            .slot_array()
            .will_overfull(record.encode_size(), self.options.fanout)
        {
            self.split(key, value, &mut guard_stack).await
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
        guard_stack: &mut Vec<BufferFrameGuard>,
    ) -> Result<()> {
        assert!(!guard_stack.is_empty());
        println!("guard_stack length = {:?}", guard_stack.len());

        let leaf_guard = guard_stack
            .pop()
            .ok_or(FloppyError::Internal("guard stack empty".to_string()))?;

        if leaf_guard.page_id() == PAGE_ID_ROOT {
            let new_left =
                self.buf_mgr.alloc_page_with_type(TreeNodeLeaf).await?;
            let new_right =
                self.buf_mgr.alloc_page_with_type(TreeNodeLeaf).await?;
            self.split_root::<IVec, LeafNode>(
                &leaf_guard,
                &new_left,
                &new_right,
                key,
                value,
            )
            .await?;
            println!("split root LeafNode, page = {:?}, new_left = {:?}, new_right = {:?}", leaf_guard.page_id(), new_left.page_id(), new_right.page_id());
            return Ok(());
        }

        let mut new_page =
            self.buf_mgr.alloc_page_with_type(TreeNodeLeaf).await?;
        self.split_node::<IVec, LeafNode>(&leaf_guard, &new_page, key, value)
            .await?;

        let new_node = LeafNode::from_page(new_page.page_ptr())?;
        let mut split_key = new_node.slot_array().min_key();

        println!(
            "split leaf node, page = {:?}, new_page = {:?}, min_key = {:?}",
            leaf_guard.page_id(),
            new_page.page_id(),
            split_key,
        );

        // add index to interior node.
        while let Some(guard) = guard_stack.pop() {
            let node = InteriorNode::from_page(guard.page_ptr())?;
            let record = Record {
                flag: 0,
                key: split_key.as_ref(),
                value: new_page.page_id(),
            };
            if node
                .slot_array()
                .will_overfull(record.encode_size(), self.options.fanout)
            {
                if guard.page_id() == PAGE_ID_ROOT {
                    let new_left = self
                        .buf_mgr
                        .alloc_page_with_type(TreeNodeInterior)
                        .await?;
                    let new_right = self
                        .buf_mgr
                        .alloc_page_with_type(TreeNodeInterior)
                        .await?;

                    self.split_root::<PageId, InteriorNode>(
                        &guard,
                        &new_left,
                        &new_right,
                        &split_key,
                        new_page.page_id(),
                    )
                    .await?;
                    println!("split root InteriorNode, page = {:?}, new_left = {:?}, new_right = {:?}", guard.page_id(), new_left.page_id(), new_right.page_id());
                    return Ok(());
                }
                new_page =
                    self.buf_mgr.alloc_page_with_type(TreeNodeInterior).await?;
                self.split_node::<PageId, InteriorNode>(
                    &guard,
                    &new_page,
                    &split_key,
                    new_page.page_id(),
                )
                .await?;
                let new_node = InteriorNode::from_page(new_page.page_ptr())?;
                split_key = new_node.slot_array().min_key();
                new_node.slot_array().set_inf_min();
                println!(
                    "split InteriorNode, page = {:?}, new_page = {:?}",
                    guard.page_id(),
                    new_page.page_id()
                );
            } else {
                node.insert(&split_key, new_page.page_id())?;
                println!("post index to InteriorNode, page = {:?}, key = {:?}, new_page = {:?}", guard.page_id(), split_key, new_page.page_id());
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
        println!(
            "split_node, before split, count = {:?}",
            node.slot_array().num_slots()
        );

        // copy original node's content to a temporary page.
        let tmp_page = PagePtr::zero_content(PAGE_SIZE)?;
        let tmp_array = SlotArray::from_data(tmp_page.data_mut());
        tmp_array.with_iter(node.slot_array().iter())?;
        let (split_key, left_iter, right_iter) = tmp_array.split_half();

        // let (split_key, left_iter, right_iter) =
        // node.slot_array().split_half();

        node.slot_array().with_iter(left_iter)?;

        let right_node = Node::from_page(new_page.page_ptr())?;
        right_node.slot_array().with_iter(right_iter)?;

        let left_count = node.slot_array().num_slots();
        let right_count = right_node.slot_array().num_slots();

        self.insert_key_for_split(
            key,
            value,
            split_key.clone(),
            node,
            right_node,
        )?;
        println!("split_node: split_key = {:?}, left count = {:?}, right count = {:?}", split_key.as_ref(), left_count, right_count);
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

        if new_right_page.page_ptr().page_type() == TreeNodeInterior {
            // Interior node's split will move up the split key.
            // So we don't insert the split key here; and we need to
            // set the inf min flag.
            new_right_node.slot_array().set_inf_min();
        } else {
            // leaf node's split will copy up the split key.
            // SO we need to insert the split key here.
            self.insert_key_for_split(
                key,
                value,
                split_key.clone(),
                new_left_node,
                new_right_node,
            )?;
        }

        node.slot_array().reset_zero();
        guard.page_ptr().set_page_type(TreeNodeInterior);
        let root = InteriorNode::from_page(guard.page_ptr())?;
        root.init(
            split_key.as_ref(),
            new_left_page.page_id(),
            new_right_page.page_id(),
        )?;
        println!("split root, split_key = {:?}", split_key.as_ref());
        Ok(())
    }

    fn insert_key_for_split<'a, V, Node>(
        &self,
        key: &'a [u8],
        value: V,
        split_key: IVec,
        left: Node,
        right: Node,
    ) -> Result<()>
    where
        V: NodeValue,
        Node: TreeNode<'a, &'a [u8], V>,
    {
        let cmp = key.cmp(split_key.as_ref());
        if cmp == Ordering::Less {
            left.insert(key, value)
        } else if cmp == Ordering::Greater {
            right.insert(key, value)
        } else {
            Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "key already exists {key:?}"
            ))))
        }
    }
}

#[derive(Eq, PartialEq, Clone, Copy)]
enum AccessMode {
    Read,
    Insert(usize),
    Delete,
}

#[derive(Default)]
pub(crate) struct TreeOptions {
    fanout: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::sim::{SimEnv, SIM_PATH};

    async fn build_tree(options: TreeOptions) -> Result<Tree<SimEnv>> {
        let env = SimEnv;
        Tree::open(SIM_PATH, env, options).await
    }

    async fn batch_insert_and_get(
        tree: &Tree<SimEnv>,
        range: usize,
    ) -> Result<()> {
        for i in 0..range {
            let b = &i.to_le_bytes();
            tree.insert(b, b).await?;
        }

        for i in 0..range {
            let b = &i.to_le_bytes();
            let v = tree
                .get(b)
                .await?
                .unwrap_or_else(|| panic!("should not be none, key = {i}"));
            assert_eq!(b, v.as_ref());
        }
        Ok(())
    }

    async fn insert_and_get(tree: &Tree<SimEnv>, range: usize) -> Result<()> {
        for i in 0..range {
            let b = &i.to_le_bytes();
            tree.insert(b, b).await?;
            let v = tree
                .get(b)
                .await?
                .unwrap_or_else(|| panic!("should not be none, key = {i}"));
            assert_eq!(b, v.as_ref());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_tree_simple() -> Result<()> {
        let tree = build_tree(TreeOptions::default()).await?;
        batch_insert_and_get(&tree, 200).await
    }

    #[tokio::test]
    async fn test_tree_small_fanout() -> Result<()> {
        let tree = build_tree(TreeOptions { fanout: Some(3) }).await?;
        insert_and_get(&tree, 200).await
    }

    #[tokio::test]
    #[ignore]
    async fn test_tree_small_fanout_batch() -> Result<()> {
        let tree = build_tree(TreeOptions { fanout: Some(4) }).await?;
        batch_insert_and_get(&tree, 200).await
    }
}
