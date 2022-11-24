use crate::common::error::Result;
use crate::dc::buffer_pool::PageFrame;

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
    pub fn new(page_frame: &'a PageFrame) -> Result<Self> {
        let data = page_frame.data();
        let page_type = u8::from_be_bytes(data[0..1].try_into().unwrap());
        match page_type {
            PAGE_TYPE_LEAF => Ok(Self::Leaf(LeafNode::new(page_frame))),
            _ => todo!(),
        }
    }
}

struct LeafNode<'a> {
    page_frame: &'a PageFrame,
}

impl<'a> LeafNode<'a> {
    pub fn new(page_frame: &'a PageFrame) -> Self {
        Self { page_frame }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
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
