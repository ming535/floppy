/// There are several concepts related to get an item in a page.
/// - [`PageOffset`] is the byte offset within a page.
/// - [`LinePointer`] is a pointer on a page which contains a [`PageOffset`].
/// - [`LpOffset`] is a 1 based index into an array of [`LinePointer`].
use crate::dc2::page::PAGE_SIZE;
use std::mem;

/// PageOffset is the byte offset within a page starts at 0.
/// Only 15 bits are used, see [`LinePointer`] definition below.
/// So in theory, the maximus page size is 32 KB, we use 8 KB by default.
pub type PageOffset = u16;

/// A line pointer on a page. The 32 bit is arranged as:
///
/// | -- 15 bit loc_idx -- | -- 2 bit lp_flags -- | -- 15 bit tup_len -- |
///
/// lp_off: byte offset to tuple from start of the page.
/// lp_flags: state of the line pointer, see below.
/// lp_len: byte length of tuple.
pub struct LinePointer(u32);

/// lp_flags has these possible states. An UNUSED line pointer is
/// available for immediate re-use, other states are not.
const LP_UNUSED: u32 = 0;
// unused, should always have lp_len = 0
const LP_NORMAL: u32 = 1;
// used, should always have lp_len > 0
const LP_DEAD: u32 = 3;

#[derive(Eq, PartialEq, Debug)]
pub enum LinePointerFlag {
    Unused,
    Normal,
    Dead,
}

impl From<u32> for LinePointerFlag {
    fn from(value: u32) -> Self {
        match value {
            0 => LinePointerFlag::Unused,
            1 => LinePointerFlag::Normal,
            3 => LinePointerFlag::Dead,
            _ => panic!("invalid line pointer: {value}"),
        }
    }
}

impl From<LinePointerFlag> for u32 {
    fn from(value: LinePointerFlag) -> Self {
        match value {
            LinePointerFlag::Unused => 0,
            LinePointerFlag::Normal => 1,
            LinePointerFlag::Dead => 3,
        }
    }
}

impl From<u32> for LinePointer {
    fn from(value: u32) -> Self {
        LinePointer(value)
    }
}

impl LinePointer {
    pub fn new(
        page_offset: PageOffset,
        flag: LinePointerFlag,
        slot_len: usize,
    ) -> Self {
        let flag: u32 = flag.into();
        let page_offset = page_offset as u32;
        let tuple_len = slot_len as u32;
        LinePointer(page_offset << 17 | flag << 15 | tuple_len)
    }

    pub fn to_le_bytes(lp: LinePointer) -> [u8; 4] {
        u32::to_le_bytes(lp.0)
    }

    pub fn page_offset(&self) -> PageOffset {
        (self.0 >> 17) as PageOffset
    }

    pub fn lp_flag(&self) -> LinePointerFlag {
        (self.0 & 0x00018000 >> 15).into()
    }

    pub fn slot_len(&self) -> usize {
        (self.0 & 0x7fff) as usize
    }
}

/// SlotId is the 1 based index into line pointer array
/// in the header of each disk page.
pub(crate) type SlotId = u16;

const INVALID_OFFSET_NUMBER: SlotId = 0;
const FIRST_OFFSET_NUMBER: SlotId = 1;
const MAX_OFFSET_NUMBER: SlotId =
    (PAGE_SIZE / mem::size_of::<LinePointer>()) as SlotId;

pub(crate) fn is_valid_slot_id(slot_id: SlotId) -> bool {
    slot_id != INVALID_OFFSET_NUMBER && slot_id <= MAX_OFFSET_NUMBER
}
