use crate::common::error::Result;
use crate::dc::page::{PageId, PagePtr};
use std::path::Path;
use std::ptr::NonNull;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

/// BufferPool manages the in memory cache AND file usage of pages.
///
/// Every on disk page belongs to one of the following categories:
/// 1. Page0: The first page of the file. It contains a freelist page header.
/// 2. Freelist pages: The pages that is not used.
/// 3. Active pages: The pages that store the BTree.
/// 4. Unallocated pages: The pages that is not allocated yet, it is beyond the
/// end of the file.
///
/// The memory of the `BufferPool` is tracked by `PageFrame`. The `PageFrame`
/// indicates a continuous memory region that can store a page's content.
///
/// We use a `Page`'s `PageId` to find its `PageFrame` in `BufferPool`. The
/// `BufferPool` use a hash table to map `PageId` to `PageFrame`.
///
/// There are several `PageFrame` list we used to track the usage of
/// `PageFrame`:
/// 1. Freelist: The free memory that can be used to store new pages. Note that.
/// this is different from a `Page`'s Freelist.
/// 2. FlushList: The pages that
/// have been modified and need to be flushed to disk.
/// 3. LruList: The pages that are tracked by the LRU algorithm.
pub(crate) struct BufMgr {
    file: File,
}

impl BufMgr {
    pub async fn open<P: AsRef<Path>>(path: P, pool_size: usize) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;

        let metadata = file.metadata().await?;
        if metadata.len() == 0 {
            // init page zero
            let page_ptr = PagePtr::zero_content()?;
            file.write_all(page_ptr.data()).await?;
        }

        Ok(Self { file })
    }

    /// Allocate a new page from buffer pool. This happens when a node in the
    /// tree splits.
    /// To allocate a page, we first check if there is a free page in the
    /// freelist. If there is, we return the page. Otherwise, we extend the
    /// file and return the new page.
    pub fn alloc_page() -> Result<PageFrame> {
        todo!()
    }

    /// Free a page in the buffer pool. This happens when a node in the tree
    /// merges.
    /// When deallocate a page, we add the page to the freelist. We do not
    /// shrink the file here.
    pub fn dealloc_page(page_id: PageId) -> Result<()> {
        todo!()
    }

    /// Flush the page content to disk.
    pub fn flush_page(page_id: PageId) -> Result<()> {
        todo!()
    }

    /// Get a page from the buffer pool. If the page is not in the buffer pool,
    /// we read it from disk
    pub fn get_and_pin(&self, page_id: PageId) -> Result<PageFrame> {
        todo!()
    }

    /// Unpin a page, so that it can be evicted from the buffer pool.
    pub fn unpin(&self, page_id: PageId) -> Result<()> {
        todo!()
    }
}

pub(crate) struct PageFrame {
    page_id: PageId,
    page_ptr: PagePtr,
    pin_count: usize,
    dirty: bool,
}

const PAGE_FRAME_PAYLOAD_OFFSET: usize = 8;

impl PageFrame {
    pub fn new(page_id: PageId, page_ptr: PagePtr) -> Self {
        Self {
            page_id: page_id,
            page_ptr,
            pin_count: 0,
            dirty: false,
        }
    }

    pub fn get_page_lsn(&self) -> u64 {
        let data = self.payload();
        u64::from_be_bytes(data[0..PAGE_FRAME_PAYLOAD_OFFSET].try_into().unwrap())
    }

    pub fn payload<'a>(&self) -> &'a [u8] {
        &self.page_ptr.data()[PAGE_FRAME_PAYLOAD_OFFSET..]
    }

    pub fn payload_mut<'a>(&mut self) -> &'a mut [u8] {
        &mut self.page_ptr.data_mut()[PAGE_FRAME_PAYLOAD_OFFSET..]
    }
}
