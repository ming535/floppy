use crate::common::{
    error::{DCError, FloppyError, Result},
    ivec::IVec,
};
use crate::dc2::{
    codec::{Codec, Decoder, Record},
    lp::SlotId,
    page::{Page, PageId},
};
use paste::paste;
use std::{cmp::Ordering, fmt, marker::PhantomData, mem};

pub(crate) trait NodeKey:
    AsRef<[u8]> + Codec + Ord + fmt::Debug
{
}

pub(crate) trait NodeValue: Codec {}

impl NodeKey for &[u8] {}

impl NodeValue for &[u8] {}

impl NodeValue for PageId {}

type TreeLevel = u32;

type NodeFlags = u16;

/// Tree node's opaque data is defined as follows:
///
/// left_sibling  - 4 bytes [`PageId`], zero if leftmost.
/// right_sibling - 4 bytes [`PageId`], zero if rightmost.
/// tree_level    - 4 bytes [`TreeLevel`], zero for leaf nodes.
/// flags         - 2 bytes [`NodeFlags`], see below.
///
/// Bits defined in [`NodeFlags`]
const BTP_LEAF: u16 = 1 << 0; // leaf page
const BTP_ROOT: u16 = 1 << 1; // root page
const BTP_META: u16 = 1 << 3; // meta page
const BTP_INCOMPLETE_SPLIT: u16 = 1 << 7; // right sibling's downlink is missing

pub(super) struct Node<'a> {
    page: &'a mut Page,
}

macro_rules! opaque_data_accessor {
    ($name:ident, $t:ty) => {
        paste! {
            #[inline(always)]
            pub fn [<get _ $name>](&self) -> $t {
                let offset = self.[<$name _offset>]();
                let opaque = self.page.opaque_data();
                $t::from_le_bytes(
                    opaque[offset..offset + mem::size_of::<$t>()]
                        .try_into()
                        .unwrap(),
                )
            }

            #[inline(always)]
            pub fn [<set _ $name>](&mut self, v: $t) {
                let offset = self.[<$name _offset>]();
                let opaque_mut = self.page.opaque_data_mut();
                opaque_mut[offset..offset + mem::size_of::<$t>()].copy_from_slice(v.to_le_bytes().as_slice());
            }
        }
    };
}

impl<'a> Node<'a> {
    pub fn from_page(page: &'a mut Page) -> Self {
        Self { page }
    }

    pub fn opaque_size() -> usize {
        2 * mem::size_of::<PageId>()
            + mem::size_of::<TreeLevel>()
            + mem::size_of::<NodeFlags>()
    }

    #[inline(always)]
    pub fn is_leaf(&self) -> bool {
        (self.get_flags() & BTP_LEAF) != 0
    }

    #[inline(always)]
    pub fn is_root(&self) -> bool {
        (self.get_flags() & BTP_LEAF) != 0
    }

    #[inline(always)]
    pub fn is_incomplete_split(&self) -> bool {
        (self.get_flags() & BTP_INCOMPLETE_SPLIT) != 0
    }

    #[inline(always)]
    pub fn is_leftmost(&self) -> bool {
        self.get_left_sibling() == 0
    }

    #[inline(always)]
    pub fn is_rightmost(&self) -> bool {
        self.get_right_sibling() == 0
    }

    opaque_data_accessor!(left_sibling, PageId);
    opaque_data_accessor!(right_sibling, PageId);
    opaque_data_accessor!(tree_level, TreeLevel);
    opaque_data_accessor!(flags, NodeFlags);

    #[inline(always)]
    fn left_sibling_offset(&self) -> usize {
        0
    }

    #[inline(always)]
    fn right_sibling_offset(&self) -> usize {
        self.left_sibling_offset() + mem::size_of::<PageId>()
    }

    #[inline(always)]
    fn tree_level_offset(&self) -> usize {
        self.right_sibling_offset() + mem::size_of::<PageId>()
    }

    #[inline(always)]
    fn flags_offset(&self) -> usize {
        self.tree_level_offset() + mem::size_of::<TreeLevel>()
    }
}

pub(super) struct NodeIterator<'a, 'b, V> {
    node: &'b Node<'a>,
    next_slot: SlotId,
    _marker: PhantomData<V>,
}

impl<'a, 'b, V> NodeIterator<'a, 'b, V> {
    fn new(node: &'b Node<'a>, next_slot: SlotId) -> Self {
        Self {
            node,
            next_slot,
            _marker: PhantomData::default(),
        }
    }
}

impl<'a, 'b, V> Iterator for NodeIterator<'a, 'b, V>
where
    V: NodeValue,
{
    type Item = (&'a [u8], V);
    fn next(&mut self) -> Option<Self::Item> {
        let max_slot = self.node.page.max_slot();
        if self.next_slot <= max_slot {
            let content = self.node.page.get_slot(self.next_slot).unwrap();
            self.next_slot += 1;
            let mut dec = Decoder::new(content);
            let record = unsafe { Record::<V>::decode_from(&mut dec) };
            Some((record.key, record.value))
        } else {
            None
        }
    }
}

pub(super) fn new_iterator<'a, 'b: 'a, V>(
    node: &'b Node<'a>,
) -> impl 'a + 'b + Iterator<Item = (&'a [u8], V)>
where
    V: NodeValue + 'a,
{
    let next_slot = first_data_slot(node);
    NodeIterator::new(node, next_slot)
}

/// Find a value in the leaf node. When [`Tree`] identifies the correct
/// leaf node, it calls this function to get the value.
/// The logic of following the right sibling ("move right") is handled
/// by [`Tree`], not here.
pub(super) fn find_in_leaf(node: &Node, target: &[u8]) -> Result<Option<IVec>> {
    match rank(node, target) {
        Err(_) => Ok(None),
        Ok(slot_id) => {
            let slot_content = node.page.get_slot(slot_id)?;
            Ok(Some(
                Record::<&[u8]>::decode_value(slot_content, target).into(),
            ))
        }
    }
}

/// Find a value ([`PageId`]) in a internal node.
/// The logic of following the right sibling ("move right") is handled
/// by [`Tree`], not here.
pub(super) fn find_in_child<K, V>(node: &Node, target: K) -> Result<V>
where
    K: NodeKey,
    V: NodeValue,
{
    let slot_id = match rank(node, target.as_ref()) {
        Err(slot) => slot,
        Ok(slot) => slot,
    };
    let slot_content = node.page.get_slot(slot_id - 1)?;
    Ok(Record::decode_value(slot_content, target.as_ref()))
}

/// Insert a pair of key value into leaf node.
/// We do not allow duplicate key.
pub(super) fn insert_leaf_node(
    node: &mut Node,
    record: Record<&[u8]>,
) -> Result<()> {
    let key = record.key;
    validate_insertion_key::<&[u8]>(node, key)?;

    match rank(node, key) {
        Err(slot_id) => node.page.insert_slot(record, slot_id),
        Ok(slot_id) => Err(FloppyError::DC(DCError::KeyAlreadyExists(
            format!("key already existed, key = {key:?}, slot_id = {slot_id:}"),
        ))),
    }
}

/// Insert into a internal node. Insertion happens when
/// a leaf node A splits into A' and A'' where A and A' has
/// the same page id, and A'' is the new page.
/// The key in [`Record`] is a high key of the child page.
/// The value in [`Record`] is a new page.
pub(super) fn insert_internal_node(
    node: &mut Node,
    record: Record<PageId>,
) -> Result<()> {
    let key = record.key;
    validate_insertion_key::<PageId>(node, key)?;

    match rank(node, key) {
        Err(slot_id) => node.page.insert_slot(record, slot_id - 1),
        Ok(slot_id) => Err(FloppyError::DC(DCError::KeyAlreadyExists(
            format!("key already existed, key = {key:?}, slot_id = {slot_id:}"),
        ))),
    }
}

pub(super) fn init_root<K>(
    node: &mut Node,
    key: &[u8],
    left_pid: PageId,
    right_pid: PageId,
) -> Result<()> {
    let minus_infinity = [0; 0];
    let first_record = Record {
        key: minus_infinity.as_slice(),
        value: left_pid,
    };
    let first_data_slot = first_data_slot(node);
    node.page.insert_slot(first_record, first_data_slot)?;

    let second_record = Record {
        key,
        value: right_pid,
    };
    node.page.insert_slot(second_record, first_data_slot + 1)
}

/// Update of high key. This happens when node split.
/// When a node A splits into A' and A'' where A and A' is the same page,
/// we update the high key of A'. The high key of A'' is the original
/// high key of A.
/// A node's high is setup when node is inited.
pub(super) fn set_high_key(node: &mut Node, key: &[u8]) -> Result<()> {
    if node.is_rightmost() {
        Err(FloppyError::Internal(
            "right most node should not update high key".to_string(),
        ))
    } else {
        let value: [u8; 0] = [0; 0];
        let record = Record {
            key,
            value: value.as_slice(),
        };
        node.page.insert_slot(record, 1)
    }
}

fn validate_insertion_key<V>(node: &Node, key: &[u8]) -> Result<()>
where
    V: NodeValue,
{
    if compare_high_key::<V>(node, key) == Ordering::Greater {
        Err(FloppyError::Internal(
            "insert a key grater than high key".to_string(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn compare_high_key<V>(node: &Node, key: &[u8]) -> Ordering
where
    V: NodeValue,
{
    if node.is_rightmost() {
        Ordering::Less
    } else {
        let slot_content = node.page.get_slot(1).unwrap();
        let high_key = Record::<&[u8]>::decode_key(slot_content);
        key.cmp(high_key)
    }
}

/// Rightmost node doesn't have a high key.
/// For non-rightmost nodes, high key is in Slot 1.
pub(super) fn high_key<'a>(node: &'a Node) -> Result<Option<&'a [u8]>> {
    if node.is_rightmost() {
        Ok(None)
    } else {
        let slot = node.page.get_slot(1)?;
        Ok(Some(slot))
    }
}

pub(super) fn first_data_slot(node: &Node) -> SlotId {
    if node.is_rightmost() {
        1
    } else {
        2
    }
}

/// Binary searches this node for a give key.
/// If the key is found then [`Result::Ok`] is returned, containing
/// the index of the matching key.
/// If key is not found then [`Result::Err`] is returned, containing
/// the index where a matching element could be inserted while maintaining
/// the sorted order.
///
/// If [`first_is_minus_infinity`] is true, we should treat the first data
/// slot's key as minus infinity (this is a internal node).
///
/// For a leaf node with key value paris:
/// (1, P1), (4, P2), (7, P3)
/// The rank of target key "5" returns a [`SlotId`] "3" which contains (7, P3).
/// The rank of target key "8" returns a [`SlotId`] "4" in [`Error`].
///
/// For a internal node with key value paris:
/// (inf_min, P1), (4, P2), (7, P3)
/// The rank of target key "5" returns [`SlotId`] "3" in [`Error`] which contains (7, P3),
/// the caller should adjust the [`SlotId`] to "2" to get a child node.
/// The rank of target key "3" returns [`SlotId`] "2" in [`Error`] which contains (4, P2),
/// the caller should adjust the [`SlotId`] to "1" to get a child node.
/// The rank of target key "4" returns [`SlotId`] "2" which contains (4, P2),
/// the caller should adjust the [`SlotId`] to "1" to get a child node.
pub(super) fn rank(
    node: &Node,
    target: &[u8],
) -> std::result::Result<SlotId, SlotId> {
    let first_is_minus_infinity = !node.is_leaf();
    let mut size = node.page.max_slot();
    let first_data_slot = first_data_slot(node);
    let mut left = first_data_slot;
    // slot_id starts with 1, `right` should be initialized with
    // `size + 1` to take into account the case where there is
    // only one slot.
    let mut right = size + 1;
    while left < right {
        let mid = left + size / 2;
        let cmp = if first_is_minus_infinity && mid == first_data_slot {
            Ordering::Greater
        } else {
            let slot_content = node.page.get_slot(mid).unwrap();
            let mut dec = Decoder::new(slot_content);
            let slot_key = unsafe { <&[u8]>::decode_from(&mut dec) };
            slot_key.cmp(target)
        };

        if cmp == Ordering::Less {
            // target > mid
            left = mid + 1;
        } else if cmp == Ordering::Greater {
            // target < mid
            right = mid;
        } else {
            return Ok(mid);
        }
        size = right - left;
    }
    Err(left)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dc2::page::PAGE_SIZE;

    fn init_single_leaf(page: &mut Page) -> Node {
        page.init(Node::opaque_size());
        let mut node = Node::from_page(page);
        node.set_left_sibling(0);
        node.set_right_sibling(0);
        node.set_tree_level(0);
        let flag = BTP_LEAF | BTP_ROOT;
        node.set_flags(flag);
        node
    }

    #[test]
    fn test_opaque() -> Result<()> {
        let mut page = Page::alloc(PAGE_SIZE)?;
        let mut node = init_single_leaf(&mut page);
        node.set_right_sibling(0);
        assert!(node.is_rightmost());
        Ok(())
    }

    #[test]
    fn test_node_rank_leaf() -> Result<()> {
        let mut page = Page::alloc(PAGE_SIZE)?;
        let node = init_single_leaf(&mut page);

        match rank(&node, b"random") {
            Err(slot_id) => assert_eq!(slot_id, 1),
            _ => panic!("this should not happen"),
        }
        Ok(())
    }

    #[test]
    fn test_insert_get() -> Result<()> {
        let mut page = Page::alloc(PAGE_SIZE)?;
        let mut node = init_single_leaf(&mut page);
        let vec = [b"1", b"2", b"3"];
        for v in vec.iter() {
            insert_leaf_node(
                &mut node,
                Record {
                    key: (*v).as_slice(),
                    value: (*v).as_slice(),
                },
            )?;
        }

        for v in vec.iter() {
            let value = find_in_leaf(&node, v.as_slice())?.unwrap();
            assert_eq!(v.as_slice(), value.as_ref());
        }

        Ok(())
    }

    #[test]
    fn test_iterator() -> Result<()> {
        let mut page = Page::alloc(PAGE_SIZE)?;
        let mut node = init_single_leaf(&mut page);

        {
            let mut iter = new_iterator::<&[u8]>(&node);
            assert!(iter.next().is_none());
        }

        let vec = [b"1", b"3", b"2"];
        for v in vec.iter() {
            insert_leaf_node(
                &mut node,
                Record {
                    key: (*v).as_slice(),
                    value: (*v).as_slice(),
                },
            )?;
        }

        let mut iter = new_iterator::<&[u8]>(&node);
        assert_eq!(iter.next().unwrap().0, b"1");
        assert_eq!(iter.next().unwrap().0, b"2");
        assert_eq!(iter.next().unwrap().0, b"3");
        Ok(())
    }
}
