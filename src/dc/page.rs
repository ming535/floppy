use crate::common::error::{FloppyError, Result};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::{mem, slice};

pub(crate) const PAGE_TYPE_INTERIOR: u8 = 0x02;
pub(crate) const PAGE_TYPE_LEAF: u8 = 0x04;

#[derive(PartialEq, Debug)]
pub(crate) enum PageType {
    TreeNodeInterior,
    TreeNodeLeaf,
}

impl From<u8> for PageType {
    fn from(flag: u8) -> Self {
        match flag {
            PAGE_TYPE_INTERIOR => PageType::TreeNodeInterior,
            PAGE_TYPE_LEAF => PageType::TreeNodeLeaf,
            _ => panic!("invalid page type"),
        }
    }
}

impl From<PageType> for u8 {
    fn from(node_type: PageType) -> Self {
        match node_type {
            PageType::TreeNodeInterior => PAGE_TYPE_INTERIOR,
            PageType::TreeNodeLeaf => PAGE_TYPE_LEAF,
        }
    }
}

pub(crate) const PAGE_SIZE: usize = 4096;
pub(super) const PAGE_ID_ZERO: PageId = PageId(0);
pub(super) const PAGE_ID_ROOT: PageId = PageId(1);

/// `PageId` is the identifier of a page in the tree.
/// Pages inside the tree use `PageId` as a disk pointer
/// to identify other pages.
///
/// `PageZero` is not used by the tree.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub(crate) struct PageId(pub(crate) u32);

impl From<u32> for PageId {
    fn from(v: u32) -> Self {
        PageId(v)
    }
}

impl From<i32> for PageId {
    fn from(v: i32) -> Self {
        PageId(v as u32)
    }
}

impl From<i64> for PageId {
    fn from(v: i64) -> Self {
        PageId(v as u32)
    }
}

impl TryFrom<usize> for PageId {
    type Error = FloppyError;

    fn try_from(value: usize) -> std::result::Result<Self, Self::Error> {
        if value > u32::MAX as usize {
            Err(FloppyError::Internal(format!("page id overflow: {value}")))
        } else {
            Ok(PageId(value as u32))
        }
    }
}

impl PageId {
    pub fn pos(&self, page_size: usize) -> usize {
        self.0 as usize * page_size
    }
}

pub(crate) struct PagePtr {
    buf: NonNull<u8>,
    size: usize,
}

impl PagePtr {
    pub fn zero_content(size: usize) -> Result<Self> {
        let layout = Layout::from_size_align(size, mem::size_of::<usize>())?;
        unsafe {
            let buf = alloc_zeroed(layout);
            if buf.is_null() {
                return Err(FloppyError::External(
                    "alloc mem failed".to_string(),
                ));
            }
            let buf = NonNull::new_unchecked(buf);
            Ok(Self { buf, size })
        }
    }

    pub fn page_lsn(&self) -> u64 {
        let data = self.data();
        u64::from_le_bytes(data[0..8].try_into().unwrap())
    }

    pub fn page_type(&self) -> PageType {
        let data = self.data();
        u8::from_le_bytes(data[8..9].try_into().unwrap()).into()
    }

    pub fn set_page_type(&self, node_type: PageType) -> &Self {
        let data = self.data_mut();
        let type_flag: u8 = node_type.into();
        data[8..9].copy_from_slice(&type_flag.to_le_bytes());
        self
    }

    pub fn data<'a>(&self) -> &'a [u8] {
        unsafe { slice::from_raw_parts(self.buf.as_ptr(), self.size) }
    }

    pub fn data_mut<'a>(&self) -> &'a mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.buf.as_ptr(), self.size) }
    }

    pub fn payload_data<'a>(&self) -> &'a [u8] {
        &self.data()[9..]
    }

    pub fn payload_data_mut<'a>(&self) -> &'a mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.buf.as_ptr().add(9), self.size - 9)
        }
    }
}

impl Drop for PagePtr {
    fn drop(&mut self) {
        let layout =
            Layout::from_size_align(self.size, mem::size_of::<usize>())
                .unwrap();
        unsafe {
            dealloc(self.buf.as_ptr(), layout);
        }
    }
}

/// PageZero is the first page of a data file. It contains information
/// about the freelist pages.
///
/// OFFSET  SIZE   DESCRIPTION
/// 0       8      Page LSN.
/// 8       4      Page number of the first freelist pages.
/// 12      4      Total number of freelist pages.
struct PageZero {
    page_ptr: PagePtr,
}

impl PageZero {
    pub fn new() -> Result<Self> {
        let page_ptr = PagePtr::zero_content(PAGE_SIZE)?;
        Ok(Self { page_ptr })
    }

    pub fn freelist_page_id(&self) -> Option<PageId> {
        let data = self.page_ptr.data();
        let page_id = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if page_id == 0 {
            None
        } else {
            Some(PageId(page_id))
        }
    }

    pub fn freelist_page_count(&self) -> u32 {
        let data = self.page_ptr.data();
        u32::from_le_bytes(data[4..8].try_into().unwrap())
    }

    pub fn set_freelist_page_id(&mut self, page_id: PageId) {
        let data = self.page_ptr.data_mut();
        data[0..4].copy_from_slice(&page_id.0.to_le_bytes());
        let count = self.freelist_page_count();
        data[4..8].copy_from_slice(&count.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_ptr_read_write() -> Result<()> {
        let page = PagePtr::zero_content(PAGE_SIZE)?;
        page.data_mut()[0] = 1;
        assert_eq!(page.data()[0], 1);
        page.data_mut()[1] = 3;
        Ok(())
    }
}
