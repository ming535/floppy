use crate::common::error::Result;
use crate::dc2::codec::{Decoder, Encoder};
use crate::dc2::{
    codec::Codec,
    lp::SlotId,
    page::{Page, PageId},
};
use paste::paste;
use std::cmp::Ordering;
use std::{fmt, mem};

pub(crate) trait NodeKey:
    AsRef<[u8]> + Codec + Ord + fmt::Debug
{
}

pub(crate) trait NodeValue: Codec {}

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

pub(crate) struct Node<'a> {
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
                opaque_mut[offset..offset + mem::size_of::<$t>()].copy_from_slice(&v.to_le_bytes());
            }
        }
    };
}

impl<'a> Node<'a> {
    pub fn from_page(page: &'a mut Page) -> Self {
        Self { page }
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

    pub fn insert<K, V>(&mut self, _key: K, _value: V) -> Result<()>
    where
        K: NodeKey,
        V: NodeValue,
    {
        todo!()
    }

    pub fn get<K, V>(&self, _key: K) -> Result<Option<V>>
    where
        K: NodeKey,
        V: NodeValue,
    {
        todo!()
    }

    opaque_data_accessor!(left_sibling, PageId);
    opaque_data_accessor!(right_sibling, PageId);
    opaque_data_accessor!(tree_level, TreeLevel);
    opaque_data_accessor!(flags, NodeFlags);

    pub fn opaque_size(&self) -> usize {
        2 * mem::size_of::<PageId>()
            + mem::size_of::<TreeLevel>()
            + mem::size_of::<NodeFlags>()
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
    fn rank(
        &self,
        target: &[u8],
        first_is_minus_infinity: bool,
    ) -> std::result::Result<SlotId, SlotId> {
        let mut size = self.page.max_slot();
        let first_data_slot = self.first_data_slot();
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
                let slot_content = self.page.get_slot(mid).unwrap();
                let mut dec = Decoder::new(slot_content);
                let slot_key = unsafe { <&[u8]>::decode_from(&mut dec) };
                slot_key.cmp(&target)
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

    fn left_sibling_offset(&self) -> usize {
        0
    }

    /// Rightmost node doesn't have a high key.
    /// For non-rightmost nodes, high key is in Slot 1.
    fn high_key(&self) -> Result<Option<&[u8]>> {
        if self.is_rightmost() {
            Ok(None)
        } else {
            let slot = self.page.get_slot(1)?;
            Ok(Some(slot))
        }
    }

    fn first_data_slot(&self) -> SlotId {
        if self.is_rightmost() {
            1
        } else {
            2
        }
    }

    fn right_sibling_offset(&self) -> usize {
        self.left_sibling_offset() + mem::size_of::<PageId>()
    }

    fn tree_level_offset(&self) -> usize {
        self.right_sibling_offset() + mem::size_of::<PageId>()
    }

    fn flags_offset(&self) -> usize {
        self.tree_level_offset() + mem::size_of::<TreeLevel>()
    }
}

/// Find a value in the leaf node. When [`Tree`] identifies the correct
/// leaf node, it calls this function to get the value.
/// The logic of following the right sibling ("move right") is handled
/// by [`Tree`], not here.
pub(crate) fn find_in_leaf<K, V>(node: &Node, target: K) -> Result<Option<V>>
where
    K: NodeKey,
    V: NodeValue,
{
    match node.rank(target.as_ref(), false) {
        Err(_) => Ok(None),
        Ok(slot_id) => {
            let slot_content = node.page.get_slot(slot_id)?;
            Ok(Some(decode_value(slot_content, target.as_ref())))
        }
    }
}

/// Find a value ([`PageId`]) in a internal node.
/// The logic of following the right sibling ("move right") is handled
/// by [`Tree`], not here.
pub(crate) fn find_in_child<K, V>(node: &Node, target: K) -> Result<V>
where
    K: NodeKey,
    V: NodeValue,
{
    let slot_id = match node.rank(target.as_ref(), true) {
        Err(slot) => slot,
        Ok(slot) => slot,
    };
    let slot_content = node.page.get_slot(slot_id - 1)?;
    Ok(decode_value(slot_content, target.as_ref()))
}

pub(crate) fn insert_leaf_node<K, V>(
    node: &Node,
    key: K,
    value: V,
) -> Result<()>
where
    K: NodeKey,
    V: NodeValue,
{
    todo!()
}

pub(crate) fn insert_internal_node<K, V>(
    node: &Node,
    left_high_key: K,
    new_page: V,
) -> Result<()>
where
    K: NodeKey,
    V: NodeValue,
{
    todo!()
}

fn decode_value<V>(slot_content: &[u8], key: &[u8]) -> V
where
    V: NodeValue,
{
    let offset = key.encode_size();
    let mut decoder = Decoder::new(&slot_content[offset..]);
    unsafe { V::decode_from(&mut decoder) }
}

impl Codec for &[u8] {
    fn encode_size(&self) -> usize {
        // 2 bytes for size
        mem::size_of::<u16>() + self.len()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u16(self.len() as u16);
        enc.put_byte_slice(self);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let len = dec.get_u16() as usize;
        dec.get_byte_slice(len)
    }
}

impl NodeKey for &[u8] {}
