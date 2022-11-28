use crate::common::error::Result;
use crate::dc::buf_mgr::PageFrame;

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

    fn slot(&self, id: usize) -> &'a [u8] {
        &self.page_frame.data()[id * 2..]
    }

    fn slot_content_size(&self, key: &[u8], value: &[u8]) -> usize {
        // record_head (1 byte) | payload_size (2 byte) | key_size (2 byte) |
        // key_content | value_size (2 byte)
        7 + key.len() + value.len()
    }

    fn encode_slot_content(&self, key: &[u8], value: &[u8], buf: &mut [u8]) {
        let payload_size = self.slot_content_size(key, value);
        assert_eq!(buf.len(), payload_size);
        let mut cur_offset = 0;

        // record header
        cur_offset += 1;

        // payload size
        let s = payload_size.to_be_bytes();
        buf[cur_offset..cur_offset + 2].copy_from_slice(s.as_ref());
        cur_offset += 2;

        // key size | key
        buf[cur_offset..cur_offset + 2].copy_from_slice(key.len().to_be_bytes().as_ref());
        cur_offset += 2;
        buf[cur_offset..cur_offset + key.len()].copy_from_slice(key);
        cur_offset += key.len();

        // value size / value
        buf[cur_offset..cur_offset + 2].copy_from_slice(value.len().to_be_bytes().as_ref());
        cur_offset += 2;
        buf[cur_offset..].copy_from_slice(value);
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
