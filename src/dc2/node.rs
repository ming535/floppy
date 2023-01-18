use crate::common::{
    error::{DCError, FloppyError, Result},
    ivec::IVec,
};
use crate::dc2::lp::{LinePointer, PageOffset};
use crate::dc2::{
    codec::{Codec, Decoder, Record},
    lp::SlotId,
    opaque::opaque_data_accessor,
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

impl<'a> Node<'a> {
    pub fn from_page(page: &'a mut Page) -> Self {
        Self { page }
    }

    pub fn format_page(&mut self) {
        let opaque_size = Self::opaque_size();
        self.page.init(opaque_size);
    }

    pub fn opaque_size() -> usize {
        2 * mem::size_of::<PageId>()
            + mem::size_of::<TreeLevel>()
            + mem::size_of::<NodeFlags>()
    }

    pub fn will_overfull(&self, record_size: usize) -> bool {
        self.page.get_record_free_space() < record_size
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
pub(super) fn find_child(node: &Node, target: &[u8]) -> Result<PageId> {
    let slot_id = match rank(node, target) {
        Err(slot) => slot,
        Ok(slot) => slot,
    };
    let slot_content = node.page.get_slot(slot_id - 1)?;
    Ok(Record::decode_value(slot_content, target))
}

/// Insert a pair of key value into leaf node.
/// We do not allow duplicate key.
pub(super) fn insert_leaf_node(
    node: &mut Node,
    record: Record<&[u8]>,
) -> Result<()> {
    let key = record.key;
    validate_insertion_key(node, key)?;

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
    validate_insertion_key(node, key)?;

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

fn validate_insertion_key(node: &Node, key: &[u8]) -> Result<()> {
    if compare_high_key(node, key) == Ordering::Greater {
        Err(FloppyError::Internal(
            "insert a key grater than high key".to_string(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn compare_high_key(node: &Node, key: &[u8]) -> Ordering {
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

/// Find a split location in a node.
/// * Input
/// - `node` is [`Node`] we would like to split.
/// - `key` is the new record's key.
/// - `insert_size` is the new record's encoded size.
/// * Output
/// - The page offset where new record will be inserted.
/// - A boolean value indicates whether the new record is in left or right page.
///
/// The basic idea is to imagine that a new record is already in the node, and
/// split the node into half to make left and right page equally sized.
pub(super) fn split_location(
    node: &Node,
    key: &[u8],
    insert_size: usize,
) -> Result<SplitLocation> {
    let new_record_slot = match rank(node, key) {
        Err(s) => Ok(s),
        Ok(_) => Err(FloppyError::DC(DCError::KeyAlreadyExists(
            "key already exists when finding split location".to_string(),
        ))),
    }?;

    // 1. let space = get the total used space + new record size
    // 2. let half = space/2
    // 3. iterator through slots and calculate the accumulated size on the way,
    //    until we find a first slot when the accumulated size >= half.
    //    We need to consider the new record during the iteration. The result of
    //    this iteration is target slot, target offset.
    //    slot >= target slot goes to the right page.
    // 4.
    //
    let half_size = node.page.get_used_size() + insert_size / 2;
    let mut acc_size = 0;
    let first_data_slot = first_data_slot(node);
    let max_slot = node.page.max_slot();

    fn acc_slot_size(
        node: &Node,
        acc_size: usize,
        slot_id: SlotId,
    ) -> Result<usize> {
        let content = node.page.get_slot(slot_id)?;
        Ok(acc_size + content.len() + mem::size_of::<LinePointer>())
    }

    if new_record_slot > max_slot {
        assert_eq!(new_record_slot, max_slot + 1);
        for slot_iter in first_data_slot..=max_slot {
            acc_size = acc_slot_size(node, acc_size, slot_iter)?;
            if acc_size >= half_size {
                return Ok(SplitLocation {
                    split_slot: slot_iter,
                    new_record_slot,
                });
            }
        }
        Ok(SplitLocation {
            split_slot: new_record_slot,
            new_record_slot,
        })
    } else {
        for slot_iter in first_data_slot..new_record_slot {
            acc_size = acc_slot_size(node, acc_size, slot_iter)?;
            if acc_size >= half_size {
                return Ok(SplitLocation {
                    split_slot: slot_iter,
                    new_record_slot,
                });
            }
        }
        // haven't found the split point. handle the new record slot first.
        acc_size += insert_size + mem::size_of::<LinePointer>();
        if acc_size >= half_size {
            return Ok(SplitLocation {
                split_slot: new_record_slot,
                new_record_slot,
            });
        }

        for slot_iter in new_record_slot..=max_slot {
            acc_size = acc_slot_size(node, acc_size, slot_iter)?;
            if acc_size >= half_size {
                return Ok(SplitLocation {
                    split_slot: slot_iter,
                    new_record_slot,
                });
            }
        }
        Err(FloppyError::Internal(
            "cannot find a split point".to_string(),
        ))
    }
}

#[derive(Clone, Copy)]
pub struct SplitLocation {
    /// [1, split_slot) will be on the left node,
    /// [split_slot,..) will be on the right node.
    split_slot: SlotId,
    /// When `new_record_slot` < `split_slot`, the new record will be inserted
    /// into left node.
    /// When `new_record_slot` >= `split_slot`, the new record will be inserted
    /// into right node.
    new_record_slot: SlotId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dc2::page::PAGE_SIZE;
    use rand::{seq::SliceRandom, thread_rng};
    use std::collections::BTreeMap;

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
        let mut node = init_single_leaf(&mut page);

        match rank(&node, b"random") {
            Err(slot_id) => assert_eq!(slot_id, 1),
            _ => panic!("this should not happen"),
        }

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
        match rank(&node, b"4") {
            Err(slot_id) => assert_eq!(slot_id, 4),
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

    #[test]
    fn test_with_btree() -> Result<()> {
        let mut page = Page::alloc(PAGE_SIZE)?;
        let mut node = init_single_leaf(&mut page);

        let mut model = BTreeMap::new();

        let mut rng = thread_rng();
        let mut keys: Vec<i32> = (0..1000).collect();
        keys.shuffle(&mut rng);
        let mut values: Vec<i32> = (0..1000).collect();
        values.shuffle(&mut rng);
        let length = keys.len();
        let mut compare_size = 0usize;
        for i in 0..length {
            let key = keys[i].to_le_bytes();
            let value = values[i].to_le_bytes();
            let record = Record {
                key: key.as_slice(),
                value: value.as_slice(),
            };

            match insert_leaf_node(&mut node, record) {
                Err(FloppyError::DC(DCError::SpaceExhaustedInPage(_))) => break,
                Ok(_) => {
                    assert_eq!(model.insert(key, value), None);
                    compare_size += 1;
                }
                _ => unreachable!(),
            }
        }

        // iterator through the model.
        let model_iter = model.iter();
        let node_iter = new_iterator::<&[u8]>(&node);
        assert!(model_iter
            .eq_by(node_iter, |(mk, mv), (nk, nv)| { mk == nk && mv == nv }));
        println!("compared {compare_size} records");
        Ok(())
    }

    mod pt {
        use super::*;
        use proptest::prelude::*;
        use rand::{rngs::SmallRng, SeedableRng};

        proptest! {
            #[test]
            fn insert_and_get(v in 0..300u64) {
                let mut rng = SmallRng::seed_from_u64(v);
                let mut page = Page::alloc(PAGE_SIZE).unwrap();
                let mut node = init_single_leaf(&mut page);

                for _ in 0..v {
                    let r = rng.gen::<u32>().to_le_bytes();
                    let s = r.as_slice();
                    insert_leaf_node(&mut node, Record{key: s, value: s}).unwrap();
                    prop_assert_eq!(find_in_leaf(&node, s).unwrap().unwrap(), s);
                }
            }
        }
    }
}
