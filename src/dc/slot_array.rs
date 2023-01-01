use crate::common::error::{DCError, FloppyError, Result};
use crate::dc::{
    codec::{Codec, Decoder, Encoder},
    node::{NodeKey, NodeValue},
};
use std::{borrow::Borrow, cmp::Ordering, marker::PhantomData, mem, ops::Range, slice};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Debug)]
pub(crate) struct SlotId(pub(crate) u16);

impl From<u16> for SlotId {
    fn from(v: u16) -> Self {
        SlotId(v)
    }
}

impl TryFrom<usize> for SlotId {
    type Error = FloppyError;

    fn try_from(value: usize) -> std::result::Result<Self, Self::Error> {
        if value > u16::MAX as usize {
            Err(FloppyError::Internal(format!(
                "slot id overflow: {}",
                value
            )))
        } else {
            Ok(SlotId(value as u16))
        }
    }
}

impl From<SlotId> for usize {
    fn from(v: SlotId) -> Self {
        v.0 as usize
    }
}

pub(crate) struct SlotArray<'a, K, V> {
    data: &'a mut [u8],
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> SlotArray<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    pub fn from_data(data: &'a mut [u8]) -> Self {
        Self {
            data,
            _marker: PhantomData,
        }
    }

    pub fn reset_zero(&self) {
        unsafe {
            let ptr = self.data.as_ptr() as *mut u8;
            ptr.write_bytes(0, self.data.len());
        }
    }

    pub fn with_iter(&self, iter: impl Iterator<Item = (K, V)>) -> Result<()> {
        unsafe {
            let ptr = self.data.as_ptr() as *mut u8;
            ptr.write_bytes(0, self.data.len());
        }

        for (slot, (k, v)) in iter.enumerate() {
            self.insert_at(slot.try_into()?, k, v, None)?;
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
    pub fn rank<Q: ?Sized>(&self, target: &Q) -> std::result::Result<SlotId, SlotId>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let mut size = self.num_slots();
        let mut left = 0;
        let mut right = size;
        // (infi_small, c)
        // size = 2
        // left = 0, right = 2
        //
        // mid = 1 mid = c > target = a
        // right = 1
        // size = 1
        //
        // mid = 0
        // right = 0
        while left < right {
            let mid = left + size / 2;
            let slot_content = self.slot_content(mid.try_into().unwrap());
            let cmp = if slot_content.flag & FLAG_INFINITE_SMALL != 0 {
                Ordering::Greater
            } else {
                slot_content.key.borrow().cmp(target)
            };
            if cmp == Ordering::Less {
                // mid < target
                left = mid + 1;
            } else if cmp == Ordering::Greater {
                // mid > target
                right = mid;
            } else {
                return Ok(mid.try_into().unwrap());
            }
            size = right - left;
        }
        Err(left.try_into().unwrap())
    }

    pub fn will_overfull(&self, key: K, value: V) -> bool {
        // we need to consider the space for slot pointer.
        self.record_size(key, value) + 2 > self.free_space()
    }

    pub fn insert_at(&self, slot: SlotId, key: K, value: V, flag: Option<u8>) -> Result<()> {
        let flag = flag.map_or(0, |v| v);
        let record = Record { flag, key, value };
        let record_size = record.encode_size();
        // we need to consider slot offset.
        let size_needed = record_size + 2;
        let slot_content_start = self.slot_content_start();
        let num_slots = self.num_slots();
        let new_slot_offset = if size_needed <= self.unallocatd_space() {
            if slot_content_start == 0 {
                (self.data.len() - record_size) as u16
            } else {
                slot_content_start - record_size as u16
            }
        } else {
            // find freeblocks
            return Err(FloppyError::DC(DCError::SpaceExhaustedInPage(format!(
                "page exhausted when insert slot: {:?}",
                slot.0
            ))));
        };

        // encode slot content
        self.set_slot_content(record, new_slot_offset);

        // encode slot offset vec
        let mut slot_offset_vec = self.slot_offset_vec();
        slot_offset_vec.0.insert(slot.into(), new_slot_offset);
        self.set_slot_offset_vec(slot_offset_vec);

        // encode header
        self.set_num_slots(num_slots + 1);
        self.set_slot_content_start(new_slot_offset);
        Ok(())
    }

    pub fn update_at(&mut self, slot: SlotId, value: V) -> Result<()> {
        let mut record = self.slot_content(slot);
        record.value = value;
        let offset = self.slot_offset(slot);
        self.set_slot_content(record, offset);
        Ok(())
    }

    pub fn iter(&self) -> SlotArrayIterator<K, V> {
        SlotArrayIterator {
            node: self,
            next_slot: 0.into(),
            _marker: PhantomData,
        }
    }

    pub fn range(&self, range: Range<SlotId>) -> SlotArrayRangeIterator<K, V> {
        SlotArrayRangeIterator {
            node: self,
            next_slot: range.start,
            range,
            _marker: PhantomData,
        }
    }

    /// split_at will split the SlotArray into two SlotArrayRangeIterator left and right.
    /// The left will contain the range [0, slot), and the right will contain the range [slot, num_slots).
    pub fn split_at(
        &self,
        slot: SlotId,
    ) -> (SlotArrayRangeIterator<K, V>, SlotArrayRangeIterator<K, V>) {
        let left = self.range(SlotId(0)..slot);
        let right = self.range(slot..self.num_slots().try_into().unwrap());
        (left, right)
    }

    fn record_size(&self, key: K, value: V) -> usize {
        let record = Record {
            flag: 0,
            key,
            value,
        };
        record.encode_size()
    }

    fn free_space(&self) -> usize {
        // todo! add free block's space
        self.unallocatd_space()
    }

    fn unallocatd_space(&self) -> usize {
        let slot_content_start = self.slot_content_start() as usize;
        if slot_content_start == 0 {
            // This node haven't been used yet.
            self.data.len() - self.header_encode_size() - self.slot_offsets_size()
        } else {
            assert!(slot_content_start > self.header_encode_size() + self.slot_offsets_size());
            slot_content_start - self.header_encode_size() - self.slot_offsets_size()
        }
    }

    fn slot_offsets_size(&self) -> usize {
        2 * self.num_slots() as usize
    }

    fn freeblock(&self) -> u16 {
        let buf = unsafe { slice::from_raw_parts(self.header_free_block_ptr(), 2) };
        let mut dec = Decoder::new(buf);
        unsafe { Decoder::get_u16(&mut dec) }
    }

    fn set_freeblock(&self, freeblock: u16) {
        let buf = unsafe { slice::from_raw_parts_mut(self.header_free_block_ptr() as *mut u8, 2) };
        let mut encoder = Encoder::new(buf);
        unsafe { encoder.put_u16(freeblock) }
    }

    pub fn num_slots(&self) -> usize {
        let buf = unsafe { slice::from_raw_parts(self.header_num_slots_ptr(), 2) };
        let mut dec = Decoder::new(buf);
        unsafe { Decoder::get_u16(&mut dec) }.into()
    }

    fn set_num_slots(&self, num_slot: usize) {
        let buf = unsafe { slice::from_raw_parts_mut(self.header_num_slots_ptr() as *mut u8, 2) };
        let mut encoder = Encoder::new(buf);
        unsafe { encoder.put_u16(num_slot.try_into().unwrap()) }
    }

    fn slot_content_start(&self) -> u16 {
        let buf = unsafe { slice::from_raw_parts(self.header_slot_content_start_ptr(), 2) };
        let mut dec = Decoder::new(buf);
        unsafe { Decoder::get_u16(&mut dec) }
    }

    fn set_slot_content_start(&self, slot_content_start: u16) {
        let buf = unsafe {
            slice::from_raw_parts_mut(self.header_slot_content_start_ptr() as *mut u8, 2)
        };
        let mut encoder = Encoder::new(buf);
        unsafe { encoder.put_u16(slot_content_start) }
    }

    fn fragmented_free_bytes(&self) -> u8 {
        let buf = unsafe { slice::from_raw_parts(self.header_fragmented_free_bytes_ptr(), 1) };
        let mut dec = Decoder::new(buf);
        unsafe { Decoder::get_u8(&mut dec) }
    }

    fn set_fragmented_free_bytes(&self, fragmented_free_bytes: u8) {
        let buf = unsafe {
            slice::from_raw_parts_mut(self.header_fragmented_free_bytes_ptr() as *mut u8, 1)
        };
        let mut encoder = Encoder::new(buf);
        unsafe { encoder.put_u8(fragmented_free_bytes) }
    }

    pub fn slot_content(&self, slot: SlotId) -> Record<K, V> {
        assert!(slot < self.num_slots().try_into().unwrap());
        let offset = self.slot_offset(slot);
        let buf = &self.data[offset as usize..];
        let mut dec = Decoder::new(buf);
        unsafe { Record::decode_from(&mut dec) }
    }

    fn set_slot_content(&self, record: Record<K, V>, offset: u16) {
        let data_ptr = self.data.as_ptr() as *mut u8;
        let mut_buf = unsafe { slice::from_raw_parts_mut(data_ptr, self.data.len()) };
        let content_buf = &mut mut_buf[offset as usize..offset as usize + record.encode_size()];
        let mut enc = Encoder::new(content_buf);
        unsafe {
            record.encode_to(&mut enc);
        }
    }

    fn slot_offset_vec(&self) -> SlotOffsetVec {
        let ptr = self.slot_offset_vec_ptr();
        let buf = unsafe { slice::from_raw_parts(ptr, self.slot_offsets_size()) };
        let mut dec = Decoder::new(buf);
        unsafe { SlotOffsetVec::decode_from(&mut dec) }
    }

    fn set_slot_offset_vec(&self, offset_vec: SlotOffsetVec) {
        let ptr = self.slot_offset_vec_ptr() as *mut u8;
        let buf = unsafe { slice::from_raw_parts_mut(ptr, offset_vec.encode_size()) };
        let mut offset_vec_enc = Encoder::new(buf);
        unsafe {
            offset_vec.encode_to(&mut offset_vec_enc);
        }
    }

    fn slot_offset_vec_ptr(&self) -> *const u8 {
        let data_ptr = self.data.as_ptr();
        unsafe { data_ptr.add(self.header_encode_size()) }
    }

    fn slot_offset(&self, slot: SlotId) -> u16 {
        let buf = unsafe { slice::from_raw_parts(self.slot_offset_ptr(slot), 2) };
        let mut dec = Decoder::new(buf);
        unsafe { Decoder::get_u16(&mut dec) }
    }

    fn header_encode_size(&self) -> usize {
        // 2 bytes freeblock
        // 2 bytes num_slots
        // 2 bytes slot_content_start
        // 1 byte fragmented_free_bytes
        2 + 2 + 2 + 1
    }

    fn header_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn header_free_block_ptr(&self) -> *const u8 {
        self.header_ptr()
    }

    fn header_num_slots_ptr(&self) -> *const u8 {
        unsafe { self.header_free_block_ptr().add(2) }
    }

    fn header_slot_content_start_ptr(&self) -> *const u8 {
        unsafe { self.header_num_slots_ptr().add(2) }
    }

    fn header_fragmented_free_bytes_ptr(&self) -> *const u8 {
        unsafe { self.header_slot_content_start_ptr().add(2) }
    }

    fn sorted_array_start_ptr(&self) -> *const u8 {
        unsafe { self.header_fragmented_free_bytes_ptr().add(1) }
    }

    fn slot_offset_ptr(&self, slot: SlotId) -> *const u8 {
        unsafe { self.sorted_array_start_ptr().add(2 * usize::from(slot)) }
    }
}

pub struct SlotArrayIterator<'a, K, V> {
    node: &'a SlotArray<'a, K, V>,
    next_slot: SlotId,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for SlotArrayIterator<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot < self.node.num_slots().try_into().unwrap() {
            let slot_content = self.node.slot_content(self.next_slot);
            self.next_slot.0 += 1;
            Some((slot_content.key, slot_content.value))
        } else {
            None
        }
    }
}

pub struct SlotArrayRangeIterator<'a, K, V> {
    node: &'a SlotArray<'a, K, V>,
    next_slot: SlotId,
    range: Range<SlotId>,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for SlotArrayRangeIterator<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot > self.node.num_slots().try_into().unwrap()
            || self.next_slot >= self.range.end
        {
            None
        } else {
            let slot_content = self.node.slot_content(self.next_slot);
            self.next_slot.0 += 1;
            Some((slot_content.key, slot_content.value))
        }
    }
}

#[derive(Default)]
struct SlotOffsetVec(Vec<u16>);

impl Codec for SlotOffsetVec {
    fn encode_size(&self) -> usize {
        self.0.len() * 2
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        for offset in &self.0 {
            enc.put_u16(*offset);
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

pub(crate) const FLAG_INFINITE_SMALL: u8 = 0x1;

pub(crate) struct Record<K, V> {
    pub flag: u8,
    pub key: K,
    pub value: V,
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
    use crate::dc::page::PagePtr;

    fn init_leaf_array<F>(array: &SlotArray<&[u8], &[u8]>, f: F) -> Result<usize>
    where
        F: Fn(usize) -> usize,
    {
        let mut i: usize = 0;
        loop {
            match array.insert_at(
                i.try_into().unwrap(),
                &f(i).to_be_bytes(),
                &i.to_be_bytes(),
                None,
            ) {
                Err(FloppyError::DC(DCError::SpaceExhaustedInPage(_))) => break,
                Ok(_) => i += 1,
                Err(other) => return Err(other),
            };
        }
        assert!(array.will_overfull(&(i.to_be_bytes()), &(i.to_be_bytes())));
        Ok(i)
    }

    fn init_interior_array<F>(array: &SlotArray<&[u8], &[u8]>, f: F) -> Result<usize>
    where
        F: Fn(usize) -> usize,
    {
        let mut i: usize = 0;
        loop {
            let flag = if i == 0 {
                Some(FLAG_INFINITE_SMALL)
            } else {
                None
            };

            match array.insert_at(
                i.try_into().unwrap(),
                &f(i).to_be_bytes(),
                &i.to_be_bytes(),
                flag,
            ) {
                Err(FloppyError::DC(DCError::SpaceExhaustedInPage(_))) => break,
                Ok(_) => i += 1,
                Err(other) => return Err(other),
            };
        }
        assert!(array.will_overfull(&(i.to_be_bytes()), &(i.to_be_bytes())));
        Ok(i)
    }

    #[test]
    fn test_leaf_array_init() -> Result<()> {
        let page = PagePtr::zero_content(1024)?;
        let array = SlotArray::<&[u8], &[u8]>::from_data(page.data_mut());
        init_leaf_array(&array, |x| x)?;
        let iter = array.iter();
        for (i, (k, v)) in iter.enumerate() {
            assert_eq!(i.to_be_bytes(), k);
            assert_eq!(i.to_be_bytes(), v);
        }

        Ok(())
    }

    #[test]
    fn test_leaf_array_with_iter() -> Result<()> {
        let page_a = PagePtr::zero_content(1024)?;
        let array_a = SlotArray::<&[u8], &[u8]>::from_data(page_a.data_mut());
        let size = init_leaf_array(&array_a, |x| x)?;

        let page_b = PagePtr::zero_content(1024)?;
        let array_b = SlotArray::<&[u8], &[u8]>::from_data(page_b.data_mut());

        array_a.with_iter(array_b.iter())?;
        // array_a should be empty now.
        let mut iter_a = array_a.iter();
        assert!(iter_a.next().is_none());

        init_leaf_array(&array_b, |x| x * 2)?;
        array_a.with_iter(array_b.iter())?;
        let iter_a = array_a.iter();
        // array_a should be the same with array array_a
        let iter_b = array_b.iter();
        let iter = iter_a.zip(iter_b);

        for ((k_a, v_a), (k_b, v_b)) in iter {
            assert_eq!(k_a, k_b);
            assert_eq!(v_a, v_b);
        }
        Ok(())
    }

    #[test]
    fn test_interior_array() -> Result<()> {
        let page = PagePtr::zero_content(1024)?;
        let array = SlotArray::<&[u8], &[u8]>::from_data(page.data_mut());
        init_interior_array(&array, |x| x)?;
        let iter = array.iter();
        for (i, (k, v)) in iter.enumerate() {
            assert_eq!(i.to_be_bytes(), k);
            assert_eq!(i.to_be_bytes(), v);
        }

        Ok(())
    }
}
