use crate::common::error::Result;
use crate::dc::{
    codec::{Codec, Decoder, Encoder},
    node::{NodeKey, NodeValue},
};
use std::{borrow::Borrow, cmp::Ordering, marker::PhantomData, mem, ops::Range, slice};

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

    pub fn with_iter(&mut self, iter: impl Iterator<Item = (K, V)>) -> Result<()> {
        unsafe { self.data.as_mut_ptr().write_bytes(0, self.data.len()) }
        for (slot, (k, v)) in iter.enumerate() {
            self.insert_at(slot, k, v)?;
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
    pub fn rank<Q: ?Sized>(&self, target: &Q) -> std::result::Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let mut size = self.num_slots() as usize;
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;
            let slot_content = self.slot_content(mid);
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

    pub fn will_overflow(&self, key: K, value: V) -> bool {
        // we need to consider the space for slot pointer.
        self.record_size(key, value) + 2 > self.free_space()
    }

    pub fn insert_at(&self, slot: usize, key: K, value: V) -> Result<()> {
        let record = Record {
            flag: 0,
            key,
            value,
        };
        let record_size = record.encode_size();
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
            todo!()
        };

        // encode slot offset vec
        let mut slot_offset_vec = self.slot_offset_vec();
        slot_offset_vec.0.insert(slot, new_slot_offset);
        self.set_slot_offset_vec(slot_offset_vec);

        // encode content
        self.set_slot_content(record, slot);

        // encode header
        let new_num_slots = num_slots + 1;
        self.set_num_slots(new_num_slots);
        self.set_slot_content_start(new_slot_offset);

        Ok(())
    }

    pub fn update_at(&mut self, slot: usize, value: V) -> Result<()> {
        let mut record = self.slot_content(slot);
        record.value = value;
        self.set_slot_content(record, slot);
        Ok(())
    }

    pub fn iter(&self) -> SlotArrayIterator<K, V> {
        SlotArrayIterator {
            node: self,
            next_slot: 0,
            _marker: PhantomData,
        }
    }

    pub fn range(&self, range: Range<u16>) -> SlotArrayRangeIterator<K, V> {
        SlotArrayRangeIterator {
            node: self,
            next_slot: range.start,
            range,
            _marker: PhantomData,
        }
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

    pub fn num_slots(&self) -> u16 {
        let buf = unsafe { slice::from_raw_parts(self.header_num_slots_ptr(), 2) };
        let mut dec = Decoder::new(buf);
        unsafe { Decoder::get_u16(&mut dec) }
    }

    fn set_num_slots(&self, num_slot: u16) {
        let buf = unsafe { slice::from_raw_parts_mut(self.header_num_slots_ptr() as *mut u8, 2) };
        let mut encoder = Encoder::new(buf);
        unsafe { encoder.put_u16(num_slot) }
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

    pub fn slot_content(&self, slot: usize) -> Record<K, V> {
        assert!(slot < self.num_slots() as usize);
        let offset = self.slot_offset(slot as u16);
        let buf = &self.data[offset as usize..];
        let mut dec = Decoder::new(buf);
        unsafe { Record::decode_from(&mut dec) }
    }

    fn set_slot_content(&self, record: Record<K, V>, slot: usize) {
        let slot_offset = self.slot_offset(slot as u16);
        let data_ptr = self.data.as_ptr() as *mut u8;
        let mut_buf = unsafe { slice::from_raw_parts_mut(data_ptr, self.data.len()) };
        let content_buf =
            &mut mut_buf[slot_offset as usize..slot_offset as usize + record.encode_size()];
        let mut enc = Encoder::new(content_buf);
        unsafe {
            record.encode_to(&mut enc);
        }
    }

    fn slot_offset_vec(&self) -> SlotOffsetVec {
        let ptr = self.slot_offset_vec_ptrs();
        let buf = unsafe { slice::from_raw_parts(ptr, self.slot_offsets_size()) };
        let mut dec = Decoder::new(buf);
        unsafe { SlotOffsetVec::decode_from(&mut dec) }
    }

    fn set_slot_offset_vec(&self, offset_vec: SlotOffsetVec) {
        let ptr = self.slot_offset_vec_ptrs() as *mut u8;
        let buf = unsafe { slice::from_raw_parts_mut(ptr, self.slot_offsets_size()) };
        let mut offset_vec_enc = Encoder::new(buf);
        unsafe {
            offset_vec.encode_to(&mut offset_vec_enc);
        }
    }

    fn slot_offset_vec_ptrs(&self) -> *const u8 {
        let data_ptr = self.data.as_ptr();
        unsafe { data_ptr.add(self.header_encode_size()) }
    }

    fn slot_offset(&self, slot: u16) -> u16 {
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
        unsafe { self.header_free_block_ptr().add(2) }
    }

    fn header_fragmented_free_bytes_ptr(&self) -> *const u8 {
        unsafe { self.header_slot_content_start_ptr().add(2) }
    }

    fn sorted_array_start_ptr(&self) -> *const u8 {
        unsafe { self.header_fragmented_free_bytes_ptr().add(1) }
    }

    fn slot_offset_ptr(&self, slot: u16) -> *const u8 {
        unsafe { self.sorted_array_start_ptr().add(2 * slot as usize) }
    }
}

pub struct SlotArrayIterator<'a, K, V> {
    node: &'a SlotArray<'a, K, V>,
    next_slot: u16,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for SlotArrayIterator<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot < self.node.num_slots() {
            let slot_content = self.node.slot_content(self.next_slot as usize);
            self.next_slot += 1;
            Some((slot_content.key, slot_content.value))
        } else {
            None
        }
    }
}

pub struct SlotArrayRangeIterator<'a, K, V> {
    node: &'a SlotArray<'a, K, V>,
    next_slot: u16,
    range: Range<u16>,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for SlotArrayRangeIterator<'a, K, V>
where
    K: NodeKey,
    V: NodeValue,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot > self.node.num_slots() || self.next_slot >= self.range.end {
            None
        } else {
            let slot_content = self.node.slot_content(self.next_slot as usize);
            self.next_slot += 1;
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
