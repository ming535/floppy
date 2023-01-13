/// Floppy's disk page is very similar to postgres.
/// Page is arranged as:
/// - Header: used for page space management and page lsn.
/// - line pointer array: each slot pointer points to a slot.
/// - slots: each slot contains the actually contents of user's data.
/// - opaque space: used by upper layer (for example B Tree).
///
/// A page is of the form:
///  * +----------------+---------------------------------+
///  * | Header | linp1 linp2 linp3 ...                   |
///  * +-----------+----+---------------------------------+
///  * | ... linpN |                                      |
///  * +-----------+--------------------------------------+
///  * |           ^ Header#lower                         |
///  * |                                                  |
///  * |              v Header#upper                      |
///  * +-------------+------------------------------------+
///  * |             | slotN ...                          |
///  * +-------------+------------------+-----------------+
///  * |       ...    slot3 slot2 slot1 | "opaque space"  |
///  * +--------------------------------+-----------------+
///  *                                  ^ Header#opaque
///
///
///
use crate::common::error::DCError;
use crate::dc2::lp::{is_valid_slot_id, LinePointerFlag, SlotId};

use crate::{
    common::error::{FloppyError, Result},
    dc2::lp::{LinePointer, PageOffset},
};
use paste::paste;
use std::{
    alloc::{alloc, Layout},
    mem, ptr, slice,
};

pub const PAGE_SIZE: usize = 1024 * 8;

pub type PageId = u32;

type PageLsn = u64;

type PageChecksum = u16;

/// PageFlags is is not used right now.
type PageFlags = u8; // dead, may or may not have storage

/// Page header is generic to any page:
///
/// lsn        - 8 bytes
/// checksum   - 2 bytes
/// flags      - 1 byte
/// lower      - 2 bytes offset to the start of the free space.
/// upper      - 2 bytes offset to the end of free space.
/// opaque     - 2 bytes to the start of opaque space used by upper layer.
///
/// "offset" in `lower`, `upper`, `opaque` starts at 0.
/// The page's offset is in the range: [0, 1024 * 8)
/// The page's free space's offset is in the range [`lower`, `upper`).
/// The number of bytes in the free space is `upper` - `lower`.
pub struct Page {
    buf: ptr::NonNull<u8>,
    size: usize,
    inited: bool,
}

macro_rules! header_data_accessor {
    ($name:ident, $t:ty) => {
        paste! {
            #[inline]
            pub fn [<get _ $name>](&self) -> $t {
                let offset = self.[<$name _offset>]();
                let data = self.data();
                $t::from_le_bytes(
                    data[offset..offset + mem::size_of::<$t>()]
                        .try_into()
                        .unwrap(),
                )
            }

            #[inline]
            pub fn [<set _ $name>](&mut self, v: $t) {
                let offset = self.[<$name _offset>]();
                let data = self.data_mut();
                data[offset..offset + mem::size_of::<$t>()].copy_from_slice(&v.to_le_bytes());
            }
        }
    };
}

impl Page {
    pub fn alloc(size: usize) -> Result<Self> {
        let layout = Layout::from_size_align(size, mem::size_of::<usize>())?;
        unsafe {
            let buf = alloc(layout);
            if buf.is_null() {
                return Err(FloppyError::External(
                    "alloc mem failed".to_string(),
                ));
            }
            let buf = ptr::NonNull::new_unchecked(buf);
            Ok(Self {
                buf,
                size,
                inited: false,
            })
        }
    }

    pub fn init(&mut self, opaque_size: usize) {
        unsafe { ptr::write_bytes(self.buf.as_ptr(), 0, self.size) }
        self.inited = true;
        self.set_lower(self.header_size() as PageOffset);
        self.set_upper((self.size - opaque_size) as PageOffset);
        self.set_opaque((self.size - opaque_size) as PageOffset);
    }

    header_data_accessor!(lsn, PageLsn);
    header_data_accessor!(checksum, PageChecksum);
    header_data_accessor!(flags, PageFlags);
    header_data_accessor!(lower, PageOffset);
    header_data_accessor!(upper, PageOffset);
    header_data_accessor!(opaque, PageOffset);

    pub fn opaque_data(&self) -> &[u8] {
        let offset = self.opaque_offset();
        &self.data()[offset..]
    }

    pub fn opaque_data_mut(&mut self) -> &mut [u8] {
        let offset = self.opaque_offset();
        &mut self.data_mut()[offset..]
    }

    /// Insert a slot and a line pointer to this page at specific
    /// offset. If there is already a valid line pointer,
    /// it will move line pointers to the right to make space.
    pub fn insert_slot(&mut self, slot: &[u8], slot_id: SlotId) -> Result<()> {
        if slot.len() > self.get_free_space() {
            return Err(FloppyError::DC(DCError::SpaceExhaustedInPage(
                format!("page exhausted when insert slot at {slot_id:?}"),
            )));
        }
        let lower = self.get_lower();
        let upper = self.get_upper();

        // construct a new line pointer array that includes the new slot
        // and slots after the offset.
        let new_slot_offset = upper as usize - slot.len();
        let new_slot_lp = LinePointer::new(
            new_slot_offset as PageOffset,
            LinePointerFlag::Normal,
            slot.len(),
        );

        let mut new_lp_array = vec![];
        new_lp_array.extend(LinePointer::to_le_bytes(new_slot_lp));

        // copy a subset of old line pointer out to old_array
        let lp_target = self.line_pointer_offset(slot_id)? as usize;
        let old_array: Vec<u8> = self.data()[lp_target..lower as usize].into();
        // construct the new subset of line point array.
        new_lp_array.extend(old_array);

        // copy this new line point array into page
        let s = &mut self.data_mut()
            [lp_target..lower as usize + mem::size_of::<LinePointer>()];
        s.copy_from_slice(new_lp_array.as_slice());

        // copy slot into page
        let s =
            &mut self.data_mut()[upper as usize - slot.len()..upper as usize];
        s.copy_from_slice(slot);

        // update lower, upper
        self.set_lower(
            (lower as usize + mem::size_of::<LinePointer>()) as PageOffset,
        );
        self.set_upper((upper as usize - slot.len()) as PageOffset);
        Ok(())
    }

    /// Get slot based on `SlotId`
    pub fn get_slot(&self, slot_id: SlotId) -> Result<&[u8]> {
        let lp = self.line_pointer(slot_id)?;
        let offset = lp.page_offset() as usize;
        let slot_len = lp.slot_len();
        Ok(&self.data()[offset..offset + slot_len])
    }

    /// Returns the max [`SlotId`] in this page. Since [`SlotId`]
    /// starts with 1, this is also the number of slots on the page.
    /// If the page is not initialized (lower = 0), we return zero.
    pub fn max_slot(&self) -> SlotId {
        let lower = self.get_lower() as usize;
        let header_size = self.header_size();

        if lower <= header_size {
            0
        } else {
            ((lower - header_size) / mem::size_of::<LinePointer>()) as SlotId
        }
    }

    pub fn data(&self) -> &[u8] {
        if !self.inited {
            panic!("page not inited");
        }
        unsafe { slice::from_raw_parts(self.buf.as_ptr(), self.size) }
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        if !self.inited {
            panic!("page not inited");
        }
        unsafe { slice::from_raw_parts_mut(self.buf.as_ptr(), self.size) }
    }

    /// Returns the size of the free allocatable space on a page,
    /// reduced by the space needed for a new line pointer.
    fn get_free_space(&self) -> usize {
        let space = self.get_upper() - self.get_lower();
        if space < mem::size_of::<LinePointer>() as PageOffset {
            0
        } else {
            space as usize - mem::size_of::<LinePointer>()
        }
    }

    fn header_size(&self) -> usize {
        mem::size_of::<PageLsn>()
            + mem::size_of::<PageChecksum>()
            + mem::size_of::<PageFlags>()
            + 2 * mem::size_of::<PageOffset>()
    }

    #[inline(always)]
    fn line_pointer(&self, slot_id: SlotId) -> Result<LinePointer> {
        let offset = self.line_pointer_offset(slot_id)?;
        Ok(u32::from_le_bytes(
            self.data()[offset as usize
                ..offset as usize + mem::size_of::<LinePointer>()]
                .try_into()
                .unwrap(),
        )
        .into())
    }

    fn line_pointer_offset(&self, slot_id: SlotId) -> Result<PageOffset> {
        if !is_valid_slot_id(slot_id) {
            return Err(FloppyError::Internal(format!(
                "invalid slot_id {slot_id:?}"
            )));
        }
        let offset = self.lp_offset()
            + (slot_id as usize - 1) * mem::size_of::<LinePointer>();
        Ok(offset as PageOffset)
    }

    #[inline]
    fn lsn_offset(&self) -> usize {
        0
    }

    #[inline]
    fn checksum_offset(&self) -> usize {
        self.lsn_offset() + mem::size_of::<PageLsn>()
    }

    #[inline]
    fn flags_offset(&self) -> usize {
        self.checksum_offset() + mem::size_of::<PageChecksum>()
    }

    #[inline]
    fn lower_offset(&self) -> usize {
        self.flags_offset() + mem::size_of::<PageFlags>()
    }

    #[inline]
    fn upper_offset(&self) -> usize {
        self.lower_offset() + mem::size_of::<PageOffset>()
    }

    fn lp_offset(&self) -> usize {
        self.upper_offset() + mem::size_of::<PageOffset>()
    }

    #[inline]
    fn opaque_offset(&self) -> usize {
        self.upper_offset() + mem::size_of::<PageOffset>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn test_page_need_init() {
        let page_ptr = Page::alloc(PAGE_SIZE).unwrap();
        page_ptr.opaque_data();
    }

    #[test]
    fn test_page_init() -> Result<()> {
        let mut page_ptr = Page::alloc(PAGE_SIZE)?;
        page_ptr.init(0);
        page_ptr.opaque_data();
        Ok(())
    }

    #[test]
    fn test_page_insert_get() -> Result<()> {
        let mut page = Page::alloc(PAGE_SIZE)?;
        page.init(0);
        let mut i: usize = 0;
        let count_insert_asc = loop {
            match page.insert_slot(&i.to_le_bytes(), i as SlotId) {
                Err(FloppyError::DC(DCError::SpaceExhaustedInPage(_))) => {
                    break i
                }
                Ok(_) => {
                    assert_eq!(page.get_slot(i as SlotId)?, &i.to_le_bytes());
                    i += 1;
                }
                _ => unreachable!(),
            }
        };
        assert!(page.get_free_space() < 8 + 4);

        page.init(0);
        let round_size = count_insert_asc / 2;
        for i in 0..round_size {
            page.insert_slot(&i.to_le_bytes(), i as SlotId)?;
        }

        // insert with the same slot id, so we can test the movement of line pointer.
        for i in 0..round_size {
            page.insert_slot(&i.to_le_bytes(), i as SlotId)?;
        }
        Ok(())
    }
}
