use crate::common::error::{FloppyError, Result};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::{mem, slice};

const PAGE_SIZE: usize = 4096;

/// `PageId` is the identifier of a page in the tree.
/// Pages inside the tree use `PageId` as a disk pointer
/// to identify other pages.
///
/// `PageZero` is not used by the tree.
struct PageId(u32);

impl TryFrom<u32> for PageId {
    type Error = FloppyError;

    fn try_from(value: u32) -> std::result::Result<Self, Self::Error> {
        if value == 0 {
            Err(FloppyError::DC("page id should not be zero".to_string()))
        } else {
            Ok(PageId(value))
        }
    }
}

struct PagePtr {
    buf: NonNull<u8>,
    len: usize,
}

impl PagePtr {
    pub fn new() -> Result<Self> {
        let layout = Layout::from_size_align(PAGE_SIZE, mem::size_of::<usize>())?;
        unsafe {
            let buf = alloc_zeroed(layout);
            if buf.is_null() {
                return Err(FloppyError::External("alloc mem failed".to_string()));
            }
            let buf = NonNull::new_unchecked(buf);
            Ok(Self {
                buf,
                len: PAGE_SIZE,
            })
        }
    }

    pub fn data<'a>(&self) -> &'a [u8] {
        unsafe { slice::from_raw_parts(self.buf.as_ptr(), self.len) }
    }

    pub fn data_mut<'a>(&mut self) -> &'a mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.buf.as_ptr(), self.len) }
    }
}

impl Drop for PagePtr {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(PAGE_SIZE, mem::size_of::<usize>()).unwrap();
        unsafe {
            dealloc(self.buf.as_ptr(), layout);
        }
    }
}

/// PageZero is the first page of a data file. It contains information
/// about the freelist pages.
///
/// OFFSET  SIZE   DESCRIPTION
/// 0       4      Page number of the first freelist pages.
/// 4       4      Total number of freelist pages.
///
struct PageZero {
    page_ptr: PagePtr,
}

impl PageZero {
    pub fn new() -> Result<Self> {
        let page_ptr = PagePtr::new()?;
        Ok(Self { page_ptr })
    }

    pub fn freelist_page_id(&self) -> Option<PageId> {
        let data = self.page_ptr.data();
        let page_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        if page_id == 0 {
            None
        } else {
            Some(PageId(page_id))
        }
    }

    pub fn freelist_page_count(&self) -> u32 {
        let data = self.page_ptr.data();
        u32::from_be_bytes(data[4..8].try_into().unwrap())
    }

    pub fn set_freelist_page_id(&mut self, page_id: PageId) {
        let data = self.page_ptr.data_mut();
        data[0..4].copy_from_slice(&page_id.0.to_be_bytes());
        let count = self.freelist_page_count();
        data[4..8].copy_from_slice(&count.to_be_bytes());
    }
}

struct LeafPage {}

struct InteriorPage {}

struct RootPage {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_id_zero() {
        assert!(PageId::try_from(0).is_err());
    }

    #[test]
    fn page_ptr_read_write() -> Result<()> {
        let mut page = PagePtr::new()?;
        page.data_mut()[0] = 1;
        assert_eq!(page.data()[0], 1);
        page.data_mut()[1] = 3;
        Ok(())
    }
}
