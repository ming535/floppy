use crate::common::error::Result;
use crate::dc::page::{PageId, PagePtr};
use std::path::Path;

struct BufferPool {}

impl BufferPool {
    pub fn open<P: AsRef<Path>>(path: P, pool_size: usize) -> Result<Self> {
        todo!()
    }

    pub fn alloc_page() -> Result<(PageId, PagePtr)> {
        todo!()
    }

    pub fn dealloc_page(page_id: PageId) -> Result<()> {
        todo!()
    }

    pub fn flush_page(page_id: PageId) -> Result<()> {
        todo!()
    }

    pub fn get_and_pin(&self, page_id: PageId) -> Result<PagePtr> {
        todo!()
    }

    pub fn unpin(&self, page_id: PageId) -> Result<()> {
        todo!()
    }
}
