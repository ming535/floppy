use crate::common::error::{DCError, FloppyError, Result};
use crate::dc::codec::{Codec, Decoder, Encoder};
use crate::dc::node::{NodeKey, NodeValue};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem;

pub(crate) struct SlotArray<'a, K, V> {
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

    pub fn insert_at(&mut self, slot: usize, key: K, value: V) -> Result<()> {
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

    pub fn update_at(&mut self, slot: usize, value: V) -> Result<()> {
        let mut record: Record<K, V>;
        let slot_offset = self.slot_ptrs.0[slot];
        let buf = self.data[slot_offset as usize..].as_mut();
        let mut dec = Decoder::new(buf);
        unsafe {
            record = Record::decode_from(&mut dec);
            record.value = value;
        }

        let mut enc = Encoder::new(buf);
        unsafe {
            record.encode_to(&mut enc);
        }
        Ok(())
    }

    pub fn iter(&self) -> SlotArrayIterator<K, V> {
        SlotArrayIterator {
            node: self,
            next_slot: 0,
            _marker: PhantomData,
        }
    }

    pub fn num_slots(&self) -> u16 {
        self.header.num_slots
    }

    pub(crate) fn get_slot_content(&self, slot_id: usize) -> Record<K, V> {
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
