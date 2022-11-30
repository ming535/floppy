use crate::common::error::{FloppyError, Result};
use crate::dc::buf_mgr::PageFrame;
use crate::dc::codec::{Codec, Decoder, Encoder};
use std::cmp::Ordering;
use std::mem;

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
/// simplicity.
enum Node<'a> {
    Leaf(LeafNode<'a>),
    Interior(InteriorNode),
    Root(RootNode),
}

const PAGE_TYPE_ROOT: u8 = 0x01;
const PAGE_TYPE_INTERIOR: u8 = 0x02;
const PAGE_TYPE_LEAF: u8 = 0x04;

impl<'a> Node<'a> {
    pub fn new(page_frame: &'a mut PageFrame) -> Result<Self> {
        let page_type = u8::from_be(page_frame.payload()[0]);
        match page_type {
            PAGE_TYPE_LEAF => Ok(Self::Leaf(LeafNode::new(page_frame))),
            _ => todo!(),
        }
    }
}

struct LeafNode<'a> {
    page_frame: &'a mut PageFrame,
    header: NodeHeader,
    slot_array_ptrs: Vec<u16>,
}

impl<'a> LeafNode<'a> {
    pub fn new(page_frame: &'a mut PageFrame) -> Self {
        let mut dec = Decoder::new(page_frame.payload());
        let header = unsafe { NodeHeader::decode_from(&mut dec) };
        let mut slot_array_ptrs = vec![];
        for _ in 0..header.num_slots {
            unsafe {
                let slot_ptr = dec.get_u16();
                slot_array_ptrs.push(slot_ptr);
            }
        }

        Self {
            header,
            page_frame,
            slot_array_ptrs,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        match self.binary_search(key) {
            Ok(slot) => {
                let slot = self.get_slot_content(slot);
                Ok(slot.value.into())
            }
            Err(_) => Err(FloppyError::DC(format!("Key {:?} not found", key))),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        match self.binary_search(key) {
            Ok(_) => Err(FloppyError::DC(format!("Key {:?} already exists", key))),
            Err(slot) => self.put_at(slot, key, value),
        }
    }

    fn put_at(&mut self, slot: usize, key: &[u8], value: &[u8]) -> Result<()> {
        let slot_content = SlotContent {
            flag: 0,
            key,
            value,
        };
        let slot_size = slot_content.encode_size();
        let slot_offset = if slot_size <= self.unallocatd_space() {
            if self.header.slot_content_start == 0 {
                (self.page_frame.payload().encode_size() - slot_size) as u16
            } else {
                self.header.slot_content_start - slot_size as u16
            }
        } else {
            // find freeblocks
            todo!()
        };
        self.slot_array_ptrs.insert(slot, slot_offset);
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
    fn binary_search(&self, target: &[u8]) -> std::result::Result<usize, usize> {
        let mut size = self.header.num_slots as usize;
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;
            let slot_content = self.get_slot_content(mid);
            let cmp = slot_content.key.cmp(target);
            if cmp == Ordering::Less {
                left = mid + 1;
            } else if cmp == Ordering::Greater {
                right = mid;
            } else {
                return Ok(mid);
            }
        }
        Err(left)
    }

    fn get_slot_content(&self, slot_id: usize) -> SlotContent {
        assert!(slot_id < self.header.num_slots as usize);
        let offset = self.slot_array_ptrs[slot_id];
        let data = &self.page_frame.payload()[offset as usize..];
        let mut dec = Decoder::new(data);
        unsafe { SlotContent::decode_from(&mut dec) }
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

impl<'a> Iterator for LeafNode<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct InteriorNode {}

struct RootNode {}

struct NodeHeader {
    page_type: u8,
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
        enc.put_u8(self.page_type);
        enc.put_u16(self.freeblock);
        enc.put_u16(self.num_slots);
        enc.put_u16(self.slot_content_start);
        enc.put_u8(self.fragmented_free_bytes);
        if self.page_type == PAGE_TYPE_LEAF {
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
            page_type,
            freeblock,
            num_slots,
            slot_content_start,
            fragmented_free_bytes,
            right_child,
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

struct SlotContent<'a> {
    flag: u8,
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> Codec for SlotContent<'a> {
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
        let key = <&[u8]>::decode_from(dec);
        let value = <&[u8]>::decode_from(dec);
        Self { flag, key, value }
    }
}
