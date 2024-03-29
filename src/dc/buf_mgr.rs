use crate::common::error::{DCError, FloppyError, Result};
use crate::dc::page::PageType;
use crate::dc::{
    buf_frame::{BufferFrame, BufferFrameGuard},
    eviction_strategy::EvictionPool,
    page::{PageId, PagePtr, PAGE_SIZE},
};
use crate::env::*;
use dashmap::DashMap;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicI64, Ordering},
};

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
/// 1. Freelist: The free memory that can be used to store new pages.
///    Note that this is different from a `Page`'s Freelist.
/// 2. FlushList: The pages that have been modified and need to be flushed to
/// disk. 3. LruList: The pages that are tracked by the LRU algorithm.
pub(crate) struct BufMgr<E: Env> {
    env: E,
    active_pages: DashMap<PageId, BufferFrame>,
    eviction_pages: EvictionPool,
    file_path: PathBuf,
    next_page_id: AtomicI64,
}

impl<E> BufMgr<E>
where
    E: Env,
{
    /// Open the file at the given path. If the file does not exist, create it.
    /// Page 0 is initialized with an empty freelist page header.
    pub async fn open<P: AsRef<Path>>(
        env: E,
        path: P,
        pool_size: usize,
    ) -> Result<Self> {
        let file = env.open_file(path.as_ref()).await?;
        let size = file.file_size().await;
        let next_page_id = if size == 0 {
            let page_zero = PagePtr::zero_content(PAGE_SIZE)?;
            file.write_at(page_zero.data(), 0).await?;
            file.sync_all().await?;
            PageId(1)
        } else {
            PageId((size / PAGE_SIZE) as u32)
        };

        Ok(Self {
            env,
            active_pages: DashMap::new(),
            eviction_pages: EvictionPool::new(pool_size),
            file_path: path.as_ref().to_path_buf(),
            next_page_id: AtomicI64::new(next_page_id.0 as i64),
        })
    }

    pub async fn alloc_page_with_type(
        &self,
        page_type: PageType,
    ) -> Result<BufferFrameGuard> {
        let guard = self.alloc_page().await?;
        guard.page_ptr().set_page_type(page_type);
        Ok(guard)
    }

    /// Free a page in the buffer pool. This happens when a node in the tree
    /// merges.
    /// When deallocate a page, we add the page to the freelist. We do not
    /// shrink the file here.
    pub async fn dealloc_page(_page_id: PageId) -> Result<()> {
        todo!()
    }

    /// Flush the page content to disk.
    pub async fn flush_page(&self, _guard: &BufferFrameGuard) -> Result<()> {
        todo!()
    }

    /// Fix and lock a page frame in the buffer pool.
    /// "Fix" means the page won't be evicted.
    /// If the page is not in the buffer pool, we read it from disk.
    pub async fn fix_page(&self, page_id: PageId) -> Result<BufferFrameGuard> {
        if page_id >= self.next_page_id.load(Ordering::Acquire).into() {
            return Err(FloppyError::DC(DCError::PageNotFound(format!(
                "page not found, page_id = {page_id:?}"
            ))));
        }

        let entry = self.active_pages.get(&page_id);
        if let Some(entry) = entry {
            let frame = entry.value();
            Ok(BufferFrameGuard::new(frame.clone()).await)
        } else {
            let frame = self.eviction_pages.evict();
            let mut guard = BufferFrameGuard::new(frame.clone()).await;
            if guard.is_dirty() {
                self.flush_page(&guard).await?;
            }

            self.read_page(page_id, &mut guard).await?;
            self.active_pages.insert(page_id, frame.clone());
            Ok(guard)
        }
    }

    /// Allocate a new page from buffer pool. This happens when a node in the
    /// tree splits.
    /// To allocate a page, we first check if there is a free page in the
    /// freelist. If there is, we return the page. Otherwise, we extend the
    /// file and return the new page.
    async fn alloc_page(&self) -> Result<BufferFrameGuard> {
        let page_id: PageId =
            self.next_page_id.fetch_add(1, Ordering::Release).into();
        let page_ptr = PagePtr::zero_content(PAGE_SIZE)?;
        let frame = BufferFrame::new(page_id, page_ptr);
        let guard = frame.guard(None).await;
        self.active_pages.insert(page_id, frame);
        Ok(guard)
    }

    async fn read_page(
        &self,
        page_id: PageId,
        frame: &mut BufferFrameGuard,
    ) -> Result<()> {
        let file = self.env.open_file(self.file_path.as_path()).await?;
        let pos = page_id.0 as u64 * PAGE_SIZE as u64;
        match file.read_exact_at(frame.page_ptr().data_mut(), pos).await {
            Err(e) => Err(FloppyError::Io(e)),
            Ok(_) => Ok(()),
        }
    }
}
