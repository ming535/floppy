use crate::common::{
    error::{DCError, FloppyError, Result},
    ivec::IVec,
};
use crate::dc::{
    codec::{Codec, Decoder, Encoder},
    page::{PageId, PagePtr},
    slot_array::{SlotArray, SlotId, FLAG_INFINITE_SMALL},
};
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
pub(crate) const PAGE_TYPE_INTERIOR: u8 = 0x02;
pub(crate) const PAGE_TYPE_LEAF: u8 = 0x04;

#[derive(PartialEq, Debug)]
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

pub(crate) trait TreeNode<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    fn from_page(page: &'a PagePtr) -> Result<Self>
    where
        Self: Sized;

    fn get(&self, key: K) -> Result<Option<V>>;

    fn insert(&self, key: K, value: V) -> Result<()>;

    fn slot_array(&self) -> &SlotArray<'a, K, V>;
}

/// The leaf node has a slot array. Key and value are encoded in each slot.
/// It consists of the following pairs:
///
/// (K0, V0), (K1, V1), ...(Ki, Pi), ... (Kn, Vn)
pub(crate) struct LeafNode<'a> {
    array: SlotArray<'a, &'a [u8], IVec>,
}

impl<'a> LeafNode<'a> {
    pub fn min_key(&self) -> IVec {
        let record = self.array.slot_content(SlotId(0));
        IVec::from(record.key)
    }
}

impl<'a> TreeNode<'a, &'a [u8], IVec> for LeafNode<'a> {
    fn from_page(page: &'a PagePtr) -> Result<Self> {
        if page.node_type() != NodeType::Leaf {
            return Err(FloppyError::Internal(format!(
                "node type wrong, expect leaf: {:?}",
                page.node_type()
            )));
        }

        let array = SlotArray::from_data(page.node_data_mut());
        Ok(Self { array })
    }

    fn get(&self, key: &[u8]) -> Result<Option<IVec>> {
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

    fn insert(&self, key: &[u8], value: IVec) -> Result<()> where {
        match self.array.rank(key) {
            Ok(_) => Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "Key {key:?} already exists"
            )))),
            Err(slot) => self.array.insert_at(slot, key, value, None),
        }
    }

    fn slot_array(&self) -> &SlotArray<'a, &'a [u8], IVec> {
        &self.array
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
/// When a leaf page splits,we "copy" the split key and new page into the
/// parent. When a interior page splits, we "move" the split key and new page
/// into the parent.
pub(crate) struct InteriorNode<'a> {
    array: SlotArray<'a, &'a [u8], PageId>,
}

impl<'a> InteriorNode<'a> {
    pub fn set_inf_min(&self) -> IVec {
        let record = self.array.slot_content(SlotId(0));
        self.array.update_at(
            SlotId(0),
            record.key,
            record.value,
            FLAG_INFINITE_SMALL,
        );
        IVec::from(record.key)
    }

    /// Init a Interior node fro a single key and two page pointer.
    pub fn init(
        &self,
        key: &[u8],
        left_pid: PageId,
        right_pid: PageId,
    ) -> Result<()> {
        self.array.reset_zero();
        self.array.insert_at(
            SlotId(0),
            &[],
            left_pid,
            Some(FLAG_INFINITE_SMALL),
        )?;
        self.array.insert_at(SlotId(1), key, right_pid, None)?;
        Ok(())
    }
}

impl<'a> TreeNode<'a, &'a [u8], PageId> for InteriorNode<'a> {
    fn from_page(page: &'a PagePtr) -> Result<Self> {
        if page.node_type() != NodeType::Interior {
            return Err(FloppyError::Internal(format!(
                "node type wrong, expect interior: {:?}",
                page.node_type()
            )));
        }

        let array = SlotArray::from_data(page.node_data_mut());
        Ok(Self { array })
    }

    fn get(&self, key: &[u8]) -> Result<Option<PageId>> {
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
        Ok(Some(page_id))
    }

    /// Add an index where `pid` contains all keys greater all equal to
    /// `lower_bound_key`. In another words, `pid` points to keys
    /// `[lower_bound_key, next_entry_of_this_key)`.
    fn insert(&self, lower_bound_key: &'a [u8], pid: PageId) -> Result<()> {
        match self.array.rank(lower_bound_key) {
            Ok(_) => Err(FloppyError::DC(DCError::KeyAlreadyExists(format!(
                "Key {pid:?} already exists"
            )))),
            Err(pos) => {
                let slot = if pos.0 == 0 { SlotId(1) } else { pos };
                self.array.insert_at(slot, lower_bound_key, pid, None)
            }
        }
    }

    fn slot_array(&self) -> &SlotArray<'a, &'a [u8], PageId> {
        &self.array
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

impl Codec for IVec {
    fn encode_size(&self) -> usize {
        let s = self.as_ref();
        s.encode_size()
    }

    unsafe fn encode_to(&self, encoder: &mut Encoder) {
        let s = self.as_ref();
        s.encode_to(encoder)
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let len = dec.get_u16() as usize;
        let s = dec.get_byte_slice(len);
        IVec::from(s)
    }
}

impl NodeValue for IVec {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::error::Result;
    use crate::dc::page::{PagePtr, PAGE_SIZE};

    #[test]
    fn test_node_simple_leaf() -> Result<()> {
        let page_ptr = PagePtr::zero_content(PAGE_SIZE)?;
        page_ptr.set_node_type(NodeType::Leaf);
        let leaf = LeafNode::from_page(&page_ptr)?;

        leaf.insert(b"2", b"2".into())?;
        leaf.insert(b"3", b"3".into())?;
        leaf.insert(b"1", b"1".into())?;

        assert_eq!(leaf.get(b"1")?, Some(b"1".into()));
        assert_eq!(leaf.get(b"2")?, Some(b"2".into()));
        assert_eq!(leaf.get(b"8989")?, None);

        let mut iter = leaf.slot_array().iter();
        assert_eq!(iter.next(), Some((b"1".as_slice(), b"1".into())));
        assert_eq!(iter.next(), Some((b"2".as_slice(), b"2".into())));
        assert_eq!(iter.next(), Some((b"3".as_slice(), b"3".into())));
        assert_eq!(iter.next(), None);

        // build a new node and test
        let leaf = LeafNode::from_page(&page_ptr)?;
        let mut iter = leaf.slot_array().iter();
        assert_eq!(iter.next(), Some((b"1".as_slice(), b"1".into())));
        assert_eq!(iter.next(), Some((b"2".as_slice(), b"2".into())));
        assert_eq!(iter.next(), Some((b"3".as_slice(), b"3".into())));
        assert_eq!(iter.next(), None);
        Ok(())
    }

    #[test]
    fn test_node_leaf_iter() -> Result<()> {
        let page_ptr = PagePtr::zero_content(PAGE_SIZE)?;
        page_ptr.set_node_type(NodeType::Leaf);
        let leaf = LeafNode::from_page(&page_ptr)?;
        let mut idx = 0;
        loop {
            let key = format!("{idx}");
            let value = key.clone();
            match leaf.insert(key.as_bytes(), value.into()) {
                Err(_) => break,
                _ => idx += 1,
            }
        }

        Ok(())
    }

    #[test]
    fn test_node_simple_interior() -> Result<()> {
        let page_ptr = PagePtr::zero_content(PAGE_SIZE)?;
        page_ptr.set_node_type(NodeType::Interior);
        let node = InteriorNode::from_page(&page_ptr)?;
        // P1, (b), P2
        node.init(b"b", PageId(1), PageId(2))?;
        // P1, (b), P2, (c), P3
        node.insert(b"c", 3.into())?;
        // P1, (b), P2, (c), P3, (d), P8
        node.insert(b"d", 8.into())?;

        assert_eq!(node.get(b"a")?, Some(PageId(1)));

        assert_eq!(node.get(b"b")?, Some(PageId(2)));
        assert_eq!(node.get(b"b000")?, Some(PageId(2)));

        assert_eq!(node.get(b"c")?, Some(PageId(3)));
        assert_eq!(node.get(b"c000")?, Some(PageId(3)));

        assert_eq!(node.get(b"d")?, Some(PageId(8)));
        assert_eq!(node.get(b"d0")?, Some(PageId(8)));
        Ok(())
    }
}
