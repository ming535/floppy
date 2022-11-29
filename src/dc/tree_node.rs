use crate::common::error::Result;
use crate::dc::buf_mgr::PageFrame;
use crate::dc::codec::{Codec, Decoder, Encoder};
use std::mem;

/// The b-tree page header is 8 bytes in size for leaf pages and 12 bytes
/// in size for interior pages. It is composed of the following fields:
///
/// OFFSET  SIZE   DESCRIPTION
/// 0       1      The one-byte flag at offset 0 indicating the b-tree page
///                type.
///                  - 0x01: root page
///                  - 0x02: interior page
///                  - 0x04: leaf page
///                Any other value for the b-tree page type is an error.
///
/// 1       2      The two-byte integer at offset 1 gives the start of the
///                first freeblock on the page, or zero if there are no
///                freeblocks.
/// 3       2      The two-byte integer at offset 3 gives the number
///                of slots on the page.
/// 5       2      The two-byte integer at offset 5
///                designates the start of the slot content area.
///                A zero value for this integer is interpreted as 65536.
/// 7       1      The one-byte integer at offset 7 gives the number of
///                fragmented free bytes within the slot content area.
/// 8       4      The four-byte integer at offset 8 is the right-child pointer
///                for interior and root pages.                
///                Leaf pages don't have this field.
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
        let page_type = page_frame.get_page_type();
        match page_type {
            PAGE_TYPE_LEAF => Ok(Self::Leaf(LeafNode::new(page_frame))),
            _ => todo!(),
        }
    }
}

/// payload_length |
struct LeafNode<'a> {
    page_frame: &'a mut PageFrame,
}

impl<'a> LeafNode<'a> {
    pub fn new(page_frame: &'a mut PageFrame) -> Self {
        Self { page_frame }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn get_slot_content(&self, slot_id: usize) -> SlotContent {
        assert!(slot_id < self.slot_count() as usize);

        let slot_ptr = self.slot_array_ptr();
        let data = &(slot_ptr[slot_id * 2..slot_id * 2 + 2]);
        let offset = u16::from_be_bytes(data.try_into().unwrap());
        let data = &self.page_frame.data()[offset as usize..];
        let mut dec = Decoder::new(data);
        unsafe { SlotContent::decode_from(&mut dec) }
    }

    fn free_block(&self) -> u16 {
        let data = &(self.page_frame.data()[0..2]);
        u16::from_be_bytes(data.try_into().unwrap())
    }

    fn slot_count(&self) -> u16 {
        let data = &(self.page_frame.data()[2..4]);
        u16::from_be_bytes(data.try_into().unwrap())
    }

    fn slot_content_start(&self) -> u16 {
        let data = &(self.page_frame.data()[4..6]);
        u16::from_be_bytes(data.try_into().unwrap())
    }

    fn slot_array_ptr(&self) -> &[u8] {
        let count = self.slot_count() as usize;
        &self.page_frame.data()[6..6 + count]
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
//
struct NodeHeader {}

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
