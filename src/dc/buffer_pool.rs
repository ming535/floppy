use crate::common::error::Result;
use crate::dc::page::{PageId, PagePtr};

struct BufferPool {}

impl BufferPool {
    pub fn get(&self, page_id: PageId) -> Result<PagePtr> {
        todo!()
    }
}
