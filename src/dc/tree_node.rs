use crate::common::error::{DCError, FloppyError, Result};
use crate::dc::{
    buf_mgr::BufferFrame,
    codec::{Codec, Decoder, Encoder},
    page::PageId,
};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::{fmt, mem};

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
/// 8       4      The four-byte integer at offset 8 is the right-child pointer
///                for interior and root nodes.
///                Leaf nodes don't have this field filled with 0 for
///                simplicity.
const PAGE_TYPE_ROOT: u8 = 0x01;
const PAGE_TYPE_INTERIOR: u8 = 0x02;
const PAGE_TYPE_LEAF: u8 = 0x04;

pub(crate) trait NodeKey: Codec + Ord + Clone + fmt::Debug {}

pub(crate) trait NodeValue: Codec + Clone {}

pub(crate) struct TreeNode<'a, K, V> {
    page_frame: &'a mut BufferFrame,
    header: NodeHeader,
    slot_array_ptrs: SlotArrayPtr,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> TreeNode<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    pub fn new(page_frame: &'a mut BufferFrame) -> Self {
        let mut dec = Decoder::new(page_frame.payload());
        let header = unsafe { NodeHeader::decode_from(&mut dec) };
        let slot_ptr_payload = &(page_frame.payload()
            [header.encode_size()..header.encode_size() + header.num_slots as usize * 2]);
        let mut dec = Decoder::new(slot_ptr_payload);
        let slot_array_ptrs = unsafe { SlotArrayPtr::decode_from(&mut dec) };

        Self {
            header,
            page_frame,
            slot_array_ptrs,
            _marker: PhantomData,
        }
    }

    pub fn node_type(&self) -> u8 {
        self.header.node_type
    }

    pub fn get(&self, key: K) -> Result<V> {
        match self.binary_search(&key) {
            Ok(slot) => {
                let slot = self.get_slot_content(slot);
                Ok(slot.value.into())
            }
            Err(_) => Err(FloppyError::DC(DCError::KeyNotFound(format!(
                "Key {:?} not found",
                key
            )))),
        }
    }

    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        match self.binary_search(&key) {
            Ok(_) => Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "Key {:?} already exists",
                key
            )))),
            Err(slot) => self.put_at(slot, key, value),
        }
    }

    pub fn iter(&self) -> NodeIterator<K, V> {
        NodeIterator {
            node: self,
            next_slot: 0,
            _marker: PhantomData,
        }
    }

    fn put_at(&mut self, slot: usize, key: K, value: V) -> Result<()> {
        let slot_content = SlotContent {
            flag: 0,
            key,
            value,
        };
        let slot_content_size = slot_content.encode_size();
        // we need to consider the space for slot pointer.
        let slot_size = slot_content_size + 2;
        if slot_size > self.free_space() {
            return Err(FloppyError::DC(DCError::SpaceExhaustedInPage(format!(
                "No enough space to insert key {:?}",
                slot_content.key
            ))));
        }

        let slot_offset = if slot_size <= self.unallocatd_space() {
            if self.header.slot_content_start == 0 {
                (self.page_frame.payload().len() - slot_content_size) as u16
            } else {
                self.header.slot_content_start - slot_content_size as u16
            }
        } else {
            // find freeblocks
            todo!()
        };

        let buf = self.page_frame.payload_mut()[slot_offset as usize..].as_mut();
        let mut enc = Encoder::new(buf);
        unsafe {
            slot_content.encode_to(&mut enc);
        }

        // change slot array ptr, node header, and put those changes into page.
        self.slot_array_ptrs.0.insert(slot, slot_offset);
        self.header.num_slots += 1;
        self.header.slot_content_start = slot_offset;
        let header_size = self.header.encode_size();
        let mut header_enc = Encoder::new(&mut self.page_frame.payload_mut()[0..header_size]);
        let slot_ptr_buf = &mut (self.page_frame.payload_mut()
            [header_size..header_size + self.header.num_slots as usize * 2]);
        let mut slot_ptr_enc = Encoder::new(slot_ptr_buf);
        unsafe {
            self.header.encode_to(&mut header_enc);
            self.slot_array_ptrs.encode_to(&mut slot_ptr_enc);
        }
        Ok(())
    }

    /// Binary searches this node for a give key.
    ///
    /// If the key is found then [`Result::Ok`] is returned, containing
    /// the index of the matching key. If there are multiple matches, then
    /// any one of the matches could be returned.
    /// If key is not found then [`Result::Err`] is returned, containing
    /// the index where a matching element could be inserted while maintaining
    /// the sorted order.
    fn binary_search<Q: ?Sized>(&self, target: &Q) -> std::result::Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let mut size = self.header.num_slots as usize;
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;
            let slot_content = self.get_slot_content(mid);
            let cmp = slot_content.key.borrow().cmp(target);
            if cmp == Ordering::Less {
                // mid < target
                left = mid + 1;
            } else if cmp == Ordering::Greater {
                // mid > target
                right = mid;
            } else {
                return Ok(mid);
            }
            size = right - left;
        }
        Err(left)
    }

    fn get_slot_content(&self, slot_id: usize) -> SlotContent<K, V> {
        assert!(slot_id < self.header.num_slots as usize);
        let offset = self.slot_array_ptrs.0[slot_id];
        let data = &self.page_frame.payload()[offset as usize..];
        let mut dec = Decoder::new(data);
        unsafe { SlotContent::decode_from(&mut dec) }
    }

    fn free_space(&self) -> usize {
        // todo! add free block's space
        self.unallocatd_space()
    }
    fn unallocatd_space(&self) -> usize {
        let slot_content_start = self.header.slot_content_start as usize;
        if slot_content_start == 0 {
            // This node haven't been used yet.
            self.page_frame.payload().len() - self.header.encode_size() - self.slot_ptr_array_size()
        } else {
            assert!(slot_content_start > self.header.encode_size() + self.slot_ptr_array_size());
            slot_content_start - self.header.encode_size() - self.slot_ptr_array_size()
        }
    }

    fn slot_ptr_array_size(&self) -> usize {
        7 * self.header.num_slots as usize
    }
}

pub struct NodeIterator<'a, K, V> {
    node: &'a TreeNode<'a, K, V>,
    next_slot: u16,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for NodeIterator<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot < self.node.header.num_slots {
            let slot_content = self.node.get_slot_content(self.next_slot as usize);
            self.next_slot += 1;
            Some((slot_content.key, slot_content.value))
        } else {
            None
        }
    }
}

struct NodeHeader {
    node_type: u8,
    freeblock: u16,
    num_slots: u16,
    slot_content_start: u16,
    fragmented_free_bytes: u8,
    right_child: u32,
}

impl Codec for NodeHeader {
    fn encode_size(&self) -> usize {
        12
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u8(self.node_type);
        enc.put_u16(self.freeblock);
        enc.put_u16(self.num_slots);
        enc.put_u16(self.slot_content_start);
        enc.put_u8(self.fragmented_free_bytes);
        if self.node_type == PAGE_TYPE_LEAF {
            assert_eq!(self.right_child, 0);
        } else {
            assert!(self.right_child > 0);
        }
        enc.put_u32(self.right_child);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let page_type = dec.get_u8();
        let freeblock = dec.get_u16();
        let num_slots = dec.get_u16();
        let slot_content_start = dec.get_u16();
        let fragmented_free_bytes = dec.get_u8();
        let right_child = dec.get_u32();

        if page_type == PAGE_TYPE_LEAF {
            assert_eq!(right_child, 0);
        } else {
            assert!(right_child > 0);
        }

        Self {
            node_type: page_type,
            freeblock,
            num_slots,
            slot_content_start,
            fragmented_free_bytes,
            right_child,
        }
    }
}

struct SlotArrayPtr(Vec<u16>);

impl Codec for SlotArrayPtr {
    fn encode_size(&self) -> usize {
        self.0.len() * 2
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        for ptr in &self.0 {
            enc.put_u16(*ptr);
        }
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let mut vec = Vec::new();
        while dec.remaining() > 0 {
            vec.push(dec.get_u16());
        }
        Self(vec)
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

struct SlotContent<K, V> {
    flag: u8,
    key: K,
    value: V,
}

impl<K, V> Codec for SlotContent<K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    fn encode_size(&self) -> usize {
        mem::size_of::<u8>() + self.key.encode_size() + self.value.encode_size()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u8(self.flag);
        self.key.encode_to(enc);
        self.value.encode_to(enc);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let flag = dec.get_u8();
        let key = K::decode_from(dec);
        let value = V::decode_from(dec);
        Self { flag, key, value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::error::Result;
    use crate::dc::page::PagePtr;

    #[test]
    fn test_simple_put() -> Result<()> {
        let page_ptr = PagePtr::zero_content()?;
        let mut frame = BufferFrame::new(1.into(), page_ptr);
        frame.payload_mut()[0] = PAGE_TYPE_LEAF;
        let mut node: TreeNode<&[u8], &[u8]> = TreeNode::new(&mut frame);

        node.put(b"2", b"2")?;
        node.put(b"3", b"3")?;
        node.put(b"1", b"1")?;

        assert_eq!(node.get(b"1")?, b"1");
        assert_eq!(node.get(b"2")?, b"2");

        let mut iter = node.iter();
        assert_eq!(iter.next(), Some((b"1".as_ref(), b"1".as_ref())));
        assert_eq!(iter.next(), Some((b"2".as_ref(), b"2".as_ref())));
        assert_eq!(iter.next(), Some((b"3".as_ref(), b"3".as_ref())));
        assert_eq!(iter.next(), None);

        // build a new node and test
        let mut node = TreeNode::new(&mut frame);
        let mut iter = node.iter();
        assert_eq!(iter.next(), Some((b"1".as_ref(), b"1".as_ref())));
        assert_eq!(iter.next(), Some((b"2".as_ref(), b"2".as_ref())));
        assert_eq!(iter.next(), Some((b"3".as_ref(), b"3".as_ref())));
        assert_eq!(iter.next(), None);
        Ok(())
    }
}
