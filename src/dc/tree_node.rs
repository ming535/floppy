use crate::common::error::{DCError, FloppyError, Result};
use crate::dc::{
    buf_frame::BufferFrame,
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
/// 8       4092   slotted array area.
/// 4096    4      The four-byte integer at the end of a page is the right-child
///                pointer for interior and root nodes.
pub(crate) const PAGE_TYPE_ROOT: u8 = 0x01;
pub(crate) const PAGE_TYPE_INTERIOR: u8 = 0x02;
pub(crate) const PAGE_TYPE_LEAF: u8 = 0x04;

pub(crate) trait NodeKey: Codec + Ord + fmt::Debug {}

pub(crate) trait NodeValue: Codec {}

/// The leaf node is a slog array with key and value encoded.
pub(crate) struct LeafNode<'a>(SlotArray<'a, &'a [u8], &'a [u8]>);

impl<'a> LeafNode<'a> {
    pub fn from_frame(frame: &'a mut BufferFrame) -> Self {
        let slot_array = SlotArray::from_data(frame.payload_mut());
        Self(slot_array)
    }

    pub fn get(&self, key: &[u8]) -> Result<&[u8]> {
        self.0.get(key)
    }

    pub fn put(&mut self, key: &'a [u8], value: &'a [u8]) -> Result<()> {
        self.0.put(key, value)
    }

    pub fn iter(&self) -> NodeIterator<&[u8], &[u8]> {
        self.0.iter()
    }
}

/// The interior node has a slot array and a right child pointer.
pub(crate) struct InteriorNode<'a>(SlotArray<'a, &'a [u8], PageId>, u32);

impl<'a> InteriorNode<'a> {
    pub fn from_frame(frame: &'a mut BufferFrame) -> Self {
        let payload_len = frame.payload().len();
        let slot_end = payload_len - 4;
        let payload = frame.payload_mut();
        let right_child = u32::from_le_bytes(payload[slot_end - 4..slot_end].try_into().unwrap());
        let slot_array = SlotArray::from_data(&mut payload[slot_end..payload_len]);
        Self(slot_array, right_child)
    }

    pub fn get(&self, key: &[u8]) -> Result<PageId> {
        self.0.get(key)
    }

    pub fn put(&mut self, key: &'a [u8], value: PageId) -> Result<()> {
        self.0.put(key, value)
    }

    pub fn right_child(&self) -> PageId {
        self.1.into()
    }
}

struct SlotArray<'a, K, V> {
    data: &'a mut [u8],
    header: ArrayHeader,
    slot_ptrs: SlotPtrs,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> SlotArray<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    pub fn from_data(data: &'a mut [u8]) -> Self {
        let mut dec = Decoder::new(data);
        let header = unsafe { ArrayHeader::decode_from(&mut dec) };
        let header_size = header.encode_size();
        let slot_ptr_data = &(data[header_size..header_size + header.num_slots as usize * 2]);
        let mut dec = Decoder::new(slot_ptr_data);
        let slot_ptrs = unsafe { SlotPtrs::decode_from(&mut dec) };

        Self {
            data,
            header,
            slot_ptrs,
            _marker: PhantomData,
        }
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
        let record = Record {
            flag: 0,
            key,
            value,
        };
        let record_size = record.encode_size();
        // we need to consider the space for slot pointer.
        let slot_size = record_size + 2;
        if slot_size > self.free_space() {
            return Err(FloppyError::DC(DCError::SpaceExhaustedInPage(format!(
                "No enough space to insert key {:?}",
                record.key
            ))));
        }

        let slot_offset = if slot_size <= self.unallocatd_space() {
            if self.header.slot_content_start == 0 {
                (self.data.len() - record_size) as u16
            } else {
                self.header.slot_content_start - record_size as u16
            }
        } else {
            // find freeblocks
            todo!()
        };

        let buf = self.data[slot_offset as usize..slot_offset as usize + record_size].as_mut();
        let mut enc = Encoder::new(buf);
        unsafe {
            record.encode_to(&mut enc);
        }

        // change slot array ptr, node header, and put those changes into page.
        self.slot_ptrs.0.insert(slot, slot_offset);
        self.header.num_slots += 1;
        self.header.slot_content_start = slot_offset;
        let header_size = self.header.encode_size();
        let mut header_enc = Encoder::new(&mut self.data[0..header_size]);
        let slot_ptr_buf =
            &mut (self.data[header_size..header_size + self.header.num_slots as usize * 2]);
        let mut slot_ptr_enc = Encoder::new(slot_ptr_buf);
        unsafe {
            self.header.encode_to(&mut header_enc);
            self.slot_ptrs.encode_to(&mut slot_ptr_enc);
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

    fn get_slot_content(&self, slot_id: usize) -> Record<K, V> {
        assert!(slot_id < self.header.num_slots as usize);
        let offset = self.slot_ptrs.0[slot_id];
        let data = &self.data[offset as usize..];
        let mut dec = Decoder::new(data);
        unsafe { Record::decode_from(&mut dec) }
    }

    fn free_space(&self) -> usize {
        // todo! add free block's space
        self.unallocatd_space()
    }

    fn unallocatd_space(&self) -> usize {
        let slot_content_start = self.header.slot_content_start as usize;
        if slot_content_start == 0 {
            // This node haven't been used yet.
            self.data.len() - self.header.encode_size() - self.ptrs_size()
        } else {
            assert!(slot_content_start > self.header.encode_size() + self.ptrs_size());
            slot_content_start - self.header.encode_size() - self.ptrs_size()
        }
    }

    fn ptrs_size(&self) -> usize {
        2 * self.header.num_slots as usize
    }
}

pub struct NodeIterator<'a, K, V> {
    node: &'a SlotArray<'a, K, V>,
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

struct ArrayHeader {
    freeblock: u16,
    num_slots: u16,
    slot_content_start: u16,
    fragmented_free_bytes: u8,
}

impl Codec for ArrayHeader {
    fn encode_size(&self) -> usize {
        7
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u16(self.freeblock);
        enc.put_u16(self.num_slots);
        enc.put_u16(self.slot_content_start);
        enc.put_u8(self.fragmented_free_bytes);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let freeblock = dec.get_u16();
        let num_slots = dec.get_u16();
        let slot_content_start = dec.get_u16();
        let fragmented_free_bytes = dec.get_u8();

        Self {
            freeblock,
            num_slots,
            slot_content_start,
            fragmented_free_bytes,
        }
    }
}

struct SlotPtrs(Vec<u16>);

impl Codec for SlotPtrs {
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

struct Record<K, V> {
    flag: u8,
    key: K,
    value: V,
}

impl<K, V> Codec for Record<K, V>
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
    use crate::dc::{buf_frame::BufferFrame, page::PagePtr};

    #[test]
    fn test_simple_put() -> Result<()> {
        let page_ptr = PagePtr::zero_content()?;
        let mut frame = BufferFrame::new(1.into(), page_ptr);
        frame.set_page_type(PAGE_TYPE_LEAF);
        let mut leaf = LeafNode::from_frame(&mut frame);

        leaf.put(b"2", b"2")?;
        leaf.put(b"3", b"3")?;
        leaf.put(b"1", b"1")?;

        assert_eq!(leaf.get(b"1")?, b"1");
        assert_eq!(leaf.get(b"2")?, b"2");

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
}
