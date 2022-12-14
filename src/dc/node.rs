use crate::common::error::{DCError, FloppyError, Result};
use crate::dc::{
    buf_frame::{BufferFrame, BufferFrameRef},
    codec::{Codec, Decoder, Encoder},
    page::PageId,
    slot_array::{SlotArray, SlotArrayIterator},
};
use std::{cmp::Ordering, fmt, marker::PhantomData, mem};

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
    pub fn from_frame(frame: &mut BufferFrame) -> Self {
        let array = SlotArray::from_data(frame.payload_mut());
        Self { array }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        match self.array.rank(key) {
            Err(_) => Ok(None),
            Ok(idx) => {
                let record = self.array.get_slot_content(idx);
                if record.key == key {
                    Ok(Some(record.value))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn put(&mut self, key: &'a [u8], value: &'a [u8]) -> Result<()> {
        match self.array.rank(key) {
            Ok(_) => Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "Key {:?} already exists",
                key
            )))),
            Err(slot) => self.array.insert_at(slot, key, value),
        }
    }

    pub fn iter(&self) -> SlotArrayIterator<&[u8], &[u8]> {
        self.array.iter()
    }
}

/// The interior node has a slot array and extra "inf_pid" pointer.
/// It consists of the following key value pairs:
///
/// (K0, P0), (K1, P2), ...(Ki, Pi), ... (Kn, Pn), Pn+1
///
/// Each pointer represents the following key range:
///
/// (-inf, K0], (K0, K1], ..., (Ki-1, Ki], ..., (Kn-1, Kn], (Kn, +inf)
///
/// Assuming the current keys are the following:
///
/// {2, P10}, {5, P2}, {7, P15}, {9, P24}, {10, P25}, {299, P4}, {+inf, P88}
///
/// When searching for key 8, the binary_search will return index 3.
/// The pointer in index 3 covers the range (7, 9], so we can follow
/// the pointer to the child page 24.
///
/// When searching for key 310, the binary_search will return index 6.
/// The pointer index 6 is the "inf_pid", and covers the range
/// (9, +inf). So we can follow the pointer to the child page 88.
///
/// When P2 is split into two pages P2 and P2-right, where the split key is S.
/// P2: K <= S, P2-right: K > S.
/// We change the following entries:
/// 1. Add a new entry {S, P2}
/// 2. Replace the entry {5, P2} with {5, P2-right}
///
/// When P88 is split into two pages P88 and P88-right, where the split key is S.
/// P88: K <=S, P88-right: K > S.
/// We change the following entries:
/// 1. Add a new entry {S, P88}
/// 2. Replace the entry {+inf, P88} with {+inf, P88-right}
pub(crate) struct InteriorNode<'a> {
    array: SlotArray<'a, &'a [u8], PageId>,
    inf_pid: PageId,
}

impl<'a> InteriorNode<'a> {
    pub fn from_frame(frame: &mut BufferFrame) -> Self {
        let payload_len = frame.payload().len();
        let slot_end = payload_len - 4;
        let payload = frame.payload_mut();
        let inf_pid =
            u32::from_le_bytes(payload[slot_end - 4..slot_end].try_into().unwrap()).into();
        let array = SlotArray::from_data(&mut payload[..slot_end]);
        Self { array, inf_pid }
    }

    pub fn get_child(&self, key: &[u8]) -> Result<PageId> {
        let index = match self.array.rank(key) {
            Err(pos) => pos,
            Ok(pos) => pos,
        };

        let pid = if index == self.array.num_slots() as usize {
            self.inf_pid
        } else {
            self.array.get_slot_content(index).value
        };

        Ok(pid)
    }

    pub fn add_index(
        &mut self,
        split_key: &'a [u8],
        left_pid: PageId,
        right_pid: PageId,
    ) -> Result<()> {
        let pos = self.insert(split_key, left_pid)?;
        self.update(pos + 1, right_pid)
    }

    /// Insert a new entry into the interior node, and returns the rank when success.
    pub fn insert(&mut self, key: &'a [u8], value: PageId) -> Result<usize> {
        match self.array.rank(key) {
            Ok(_) => Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "Key {:?} already exists",
                key
            )))),
            Err(pos) => {
                self.array.insert_at(pos, key, value)?;
                Ok(pos)
            }
        }
    }

    pub fn update(&mut self, pos: usize, value: PageId) -> Result<()> {
        if pos == self.array.num_slots() as usize {
            self.inf_pid = value;
            Ok(())
        } else {
            self.array.update_at(pos, value)
        }
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
    use crate::dc::{buf_frame::BufferFrame, page::PagePtr};

    #[test]
    fn test_simple_leaf() -> Result<()> {
        let page_ptr = PagePtr::zero_content()?;
        let mut frame = BufferFrame::new(1.into(), page_ptr);
        frame.set_node_type(NodeType::Leaf);
        let mut leaf = LeafNode::from_frame(&mut frame);

        leaf.put(b"2", b"2")?;
        leaf.put(b"3", b"3")?;
        leaf.put(b"1", b"1")?;

        assert_eq!(leaf.get(b"1")?, Some(b"1".as_slice()));
        assert_eq!(leaf.get(b"2")?, Some(b"2".as_slice()));
        assert_eq!(leaf.get(b"8989")?, None);

        let mut iter = leaf.iter();
        assert_eq!(iter.next(), Some((b"1".as_ref(), b"1".as_ref())));
        assert_eq!(iter.next(), Some((b"2".as_ref(), b"2".as_ref())));
        assert_eq!(iter.next(), Some((b"3".as_ref(), b"3".as_ref())));
        assert_eq!(iter.next(), None);

        // build a new node and test
        let mut leaf = LeafNode::from_frame(&mut frame);
        let mut iter = leaf.iter();
        assert_eq!(iter.next(), Some((b"1".as_ref(), b"1".as_ref())));
        assert_eq!(iter.next(), Some((b"2".as_ref(), b"2".as_ref())));
        assert_eq!(iter.next(), Some((b"3".as_ref(), b"3".as_ref())));
        assert_eq!(iter.next(), None);
        Ok(())
    }

    #[test]
    fn test_simple_interior() -> Result<()> {
        let page_ptr = PagePtr::zero_content()?;
        let mut frame = BufferFrame::new(1.into(), page_ptr);
        frame.set_node_type(NodeType::Interior);
        let mut node = InteriorNode::from_frame(&mut frame);

        // P1, (2), P2
        node.add_index(b"2", 1.into(), 2.into())?;
        // P1, (2), P3, (5), P4
        node.add_index(b"5", 3.into(), 4.into())?;
        // P1, (2), P8, (3), P9, (5), P4
        node.add_index(b"3", 8.into(), 9.into())?;
        assert_eq!(node.get_child(b"1")?, PageId(1));
        assert_eq!(node.get_child(b"2")?, PageId(1));

        assert_eq!(node.get_child(b"2000")?, PageId(8));
        assert_eq!(node.get_child(b"3")?, PageId(8));

        assert_eq!(node.get_child(b"3000")?, PageId(9));
        assert_eq!(node.get_child(b"4")?, PageId(9));
        assert_eq!(node.get_child(b"5")?, PageId(9));

        assert_eq!(node.get_child(b"50")?, PageId(4));
        Ok(())
    }
}
