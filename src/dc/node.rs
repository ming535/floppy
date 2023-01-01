use crate::common::error::{DCError, FloppyError, Result};
use crate::dc::slot_array::{SlotArrayRangeIterator, FLAG_INFINITE_SMALL};
use crate::dc::{
    codec::{Codec, Decoder, Encoder},
    page::PageId,
    slot_array::{SlotArray, SlotArrayIterator, SlotId},
};
use std::{fmt, marker::PhantomData, mem};

/// The b-tree node header is 12 bytes. It is composed of the following fields:
///
/// OFFSET  SIZE   DESCRIPTION
/// 0       1      The one-byte flag at offset 0 indicating the b-tree node
///                type.
///                  - 0x01: root page
///                  - 0x02: interior page
///                  - 0x04: leaf page
///                Any other value for the b-tree node type is an error.
///
/// 1       2      The two-byte integer at offset 1 gives the start of the
///                first freeblock on the node, or zero if there are no
///                freeblocks.
/// 3       2      The two-byte integer at offset 3 gives the number
///                of slots on the node.
/// 5       2      The two-byte integer at offset 5
///                designates the start of the slot content area.
///                A zero value for this integer is interpreted as 65536.
/// 7       1      The one-byte integer at offset 7 gives the number of
///                fragmented free bytes within the slot content area.
/// 8       4092   slotted array area.
/// 4096    4      The four-byte integer at the end of a page is the right-child
///                pointer for interior and root nodes.
pub(crate) const PAGE_TYPE_INTERIOR: u8 = 0x02;
pub(crate) const PAGE_TYPE_LEAF: u8 = 0x04;

pub(crate) enum NodeType {
    Interior,
    Leaf,
}

impl From<u8> for NodeType {
    fn from(flag: u8) -> Self {
        match flag {
            PAGE_TYPE_INTERIOR => NodeType::Interior,
            PAGE_TYPE_LEAF => NodeType::Leaf,
            _ => panic!("invalid page type"),
        }
    }
}

impl From<NodeType> for u8 {
    fn from(node_type: NodeType) -> Self {
        match node_type {
            NodeType::Interior => PAGE_TYPE_INTERIOR,
            NodeType::Leaf => PAGE_TYPE_LEAF,
        }
    }
}

pub(crate) trait NodeKey: Codec + Ord + fmt::Debug {}

pub(crate) trait NodeValue: Codec {}

/// The leaf node has a slot array. Key and value are encoded in each slot.
/// It consists of the following pairs:
///
/// (K0, V0), (K1, V1), ...(Ki, Pi), ... (Kn, Vn)
pub(crate) struct LeafNode<'a> {
    array: SlotArray<'a, &'a [u8], &'a [u8]>,
}

impl<'a> LeafNode<'a> {
    pub fn from_data(data: &'a mut [u8]) -> Self {
        let array = SlotArray::from_data(data);
        Self { array }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        match self.array.rank(key) {
            Err(_) => Ok(None),
            Ok(idx) => {
                let record = self.array.slot_content(idx);
                if record.key == key {
                    Ok(Some(record.value))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn insert(&self, key: &'a [u8], value: &'a [u8]) -> Result<()> {
        match self.array.rank(key) {
            Ok(_) => Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "Key {:?} already exists",
                key
            )))),
            Err(slot) => self.array.insert_at(slot, key, value, None),
        }
    }

    pub fn will_overfull(&self, key: &[u8], value: &[u8]) -> bool {
        self.array.will_overfull(key, value)
    }

    pub fn may_underfull(&self) -> bool {
        false
    }

    pub fn with_iter(&self, iter: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> Result<()> {
        self.array.with_iter(iter)
    }

    pub fn iter(&self) -> SlotArrayIterator<&[u8], &[u8]> {
        self.array.iter()
    }

    pub fn split_iter(
        &self,
    ) -> (
        &[u8],
        SlotArrayRangeIterator<&[u8], &[u8]>,
        SlotArrayRangeIterator<&[u8], &[u8]>,
    ) {
        let num_slots = self.array.num_slots();
        assert!(num_slots > 1);
        let mid = num_slots / 2;
        let split_key = self.array.slot_content(mid.try_into().unwrap()).key;
        let (left, right) = self.array.split_at(mid.try_into().unwrap());
        (split_key, left, right)
    }
}

/// The interior node has a slot array and extra "inf_pid" pointer.
/// It consists of the following key value pairs:
///
/// (P0), (K1, P1), ...(Ki, Pi), ... (Kn, Pn)
///
/// Each pointer represents the following key range:
///      P0         P1            Pi                Pn
/// (-inf, K1), [K1, K2), ..., [Ki, Ki+1), ..., [Kn, +inf)
///
/// Take (K1, P1) as an example:
/// The key (K1) is the lower bound of the child page (P1). The upper bound
/// of the child page (P1) is the next key in the array (K2).
/// When a leaf page splits,we "copy" the split key and new page into the parent.
/// When a interior page splits, we "move" the split key and new page into the parent.
pub(crate) struct InteriorNode<'a> {
    array: SlotArray<'a, &'a [u8], PageId>,
}

impl<'a> InteriorNode<'a> {
    pub fn from_data(data: &'a mut [u8]) -> Self {
        let array = SlotArray::from_data(&mut data[4..]);
        Self { array }
    }

    /// Init a Interior node fro a single key and two page pointer.
    pub fn init(&self, key: &[u8], left_pid: PageId, right_pid: PageId) -> Result<()> {
        self.array.reset_zero();
        self.array
            .insert_at(SlotId(0), &[], left_pid, Some(FLAG_INFINITE_SMALL))?;
        self.array.insert_at(SlotId(1), key, right_pid, None)?;
        Ok(())
    }

    pub fn get_child(&self, key: &[u8]) -> Result<PageId> {
        let pos = match self.array.rank(key) {
            Err(pos) => {
                if pos.0 == 0 {
                    pos
                } else {
                    SlotId(pos.0 - 1)
                }
            }
            Ok(pos) => pos,
        };
        let page_id = self.array.slot_content(pos).value;
        Ok(page_id)
    }

    /// Add an index where `pid` contains all keys greater all equal to `lower_bound_key`.
    /// In another words, `pid` points to keys `[lower_bound_key, next_entry_of_this_key)`.
    pub fn add_index(&mut self, lower_bound_key: &'a [u8], pid: PageId) -> Result<()> {
        match self.array.rank(lower_bound_key) {
            Ok(_) => Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "Key {:?} already exists",
                pid
            )))),
            Err(pos) => {
                let slot = if pos.0 == 0 { SlotId(1) } else { pos };
                self.array.insert_at(slot, lower_bound_key, pid, None)
            }
        }
    }

    pub fn will_overfull(&self, key: &[u8]) -> bool {
        false
    }

    pub fn will_underfull(&self) -> bool {
        false
    }
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

impl NodeValue for &[u8] {}

impl Codec for PageId {
    fn encode_size(&self) -> usize {
        mem::size_of::<u32>()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u32(self.0)
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        PageId(dec.get_u32())
    }
}

impl NodeValue for PageId {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::error::Result;
    use crate::dc::page::{PagePtr, PAGE_SIZE};

    #[test]
    fn test_node_simple_leaf() -> Result<()> {
        let page_ptr = PagePtr::zero_content(PAGE_SIZE)?;
        let mut leaf = LeafNode::from_data(page_ptr.data_mut());

        leaf.insert(b"2", b"2")?;
        leaf.insert(b"3", b"3")?;
        leaf.insert(b"1", b"1")?;

        assert_eq!(leaf.get(b"1")?, Some(b"1".as_slice()));
        assert_eq!(leaf.get(b"2")?, Some(b"2".as_slice()));
        assert_eq!(leaf.get(b"8989")?, None);

        let mut iter = leaf.iter();
        assert_eq!(iter.next(), Some((b"1".as_ref(), b"1".as_ref())));
        assert_eq!(iter.next(), Some((b"2".as_ref(), b"2".as_ref())));
        assert_eq!(iter.next(), Some((b"3".as_ref(), b"3".as_ref())));
        assert_eq!(iter.next(), None);

        // build a new node and test
        let mut leaf = LeafNode::from_data(page_ptr.data_mut());
        let mut iter = leaf.iter();
        assert_eq!(iter.next(), Some((b"1".as_ref(), b"1".as_ref())));
        assert_eq!(iter.next(), Some((b"2".as_ref(), b"2".as_ref())));
        assert_eq!(iter.next(), Some((b"3".as_ref(), b"3".as_ref())));
        assert_eq!(iter.next(), None);
        Ok(())
    }

    #[test]
    fn test_node_leaf_iter() -> Result<()> {
        let page_ptr = PagePtr::zero_content(PAGE_SIZE)?;
        let mut leaf = LeafNode::from_data(page_ptr.data_mut());
        let mut idx = 0;
        loop {
            let key = format!("{}", idx);
            let value = key.clone();
            match leaf.insert(key.as_bytes(), value.as_bytes()) {
                Err(_) => break,
                _ => idx += 1,
            }
        }

        Ok(())
    }

    #[test]
    fn test_node_simple_interior() -> Result<()> {
        let page_ptr = PagePtr::zero_content(PAGE_SIZE)?;
        let mut node = InteriorNode::from_data(page_ptr.data_mut());
        // P1, (b), P2
        node.init(b"b", PageId(1), PageId(2))?;
        // P1, (b), P2, (c), P3
        node.add_index(b"c", 3.into())?;
        // P1, (b), P2, (c), P3, (d), P8
        node.add_index(b"d", 8.into())?;

        assert_eq!(node.get_child(b"a")?, PageId(1));

        assert_eq!(node.get_child(b"b")?, PageId(2));
        assert_eq!(node.get_child(b"b000")?, PageId(2));

        assert_eq!(node.get_child(b"c")?, PageId(3));
        assert_eq!(node.get_child(b"c000")?, PageId(3));

        assert_eq!(node.get_child(b"d")?, PageId(8));
        assert_eq!(node.get_child(b"d0")?, PageId(8));
        Ok(())
    }
}
