use crate::common::error::{DCError, FloppyError, Result};

use crate::dc2::{
    buf::{Buffer, PinGuard},
    eviction::EvictionPool,
    page::{Page, PageId, PAGE_SIZE},
};
use crate::env::*;
use dashmap::DashMap;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU32, Ordering},
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
    active_pages: DashMap<PageId, Buffer>,
    eviction_pages: EvictionPool,
    file_path: PathBuf,
    next_page_id: AtomicU32,
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
            let mut meta_page = Page::alloc(PAGE_SIZE)?;
            meta_page.init(0);

            file.write_at(meta_page.data(), 0).await?;
            file.sync_all().await?;
            1
        } else {
            size / PAGE_SIZE
        };

        Ok(Self {
            env,
            active_pages: DashMap::new(),
            eviction_pages: EvictionPool::new(pool_size),
            file_path: path.as_ref().to_path_buf(),
            next_page_id: AtomicU32::new(next_page_id as u32),
        })
    }

    /// Allocate a new page from buffer pool. This happens when a node in the
    /// tree splits.
    /// To allocate a page, we first check if there is a free page in the
    /// freelist. If there is, we return the page. Otherwise, we extend the
    /// file and return the new page.
    pub async fn alloc_page(&self) -> Result<PinGuard> {
        let page_id: PageId = self.next_page_id.fetch_add(1, Ordering::Release);
        let page = Page::alloc(PAGE_SIZE)?;
        let buf = Buffer::new(page_id, page);
        let pin_guard = buf.pin();
        self.active_pages.insert(page_id, buf);
        Ok(pin_guard)
    }

    /// Free a page in the buffer pool. This happens when a node in the tree
    /// merges.
    /// When deallocate a page, we add the page to the freelist. We do not
    /// shrink the file here.
    pub async fn dealloc_page(_page_id: PageId) -> Result<()> {
        todo!()
    }

    /// Flush the page content to disk.
    pub async fn flush_page(&self, _page: &Page) -> Result<()> {
        todo!()
    }

    /// Fix and lock a page frame in the buffer pool.
    /// "Fix" means the page won't be evicted.
    /// If the page is not in the buffer pool, we read it from disk.
    pub async fn fix_page(&self, page_id: PageId) -> Result<PinGuard> {
        if page_id >= self.next_page_id.load(Ordering::Acquire) {
            return Err(FloppyError::DC(DCError::PageNotFound(format!(
                "page not found, page_id = {page_id:?}"
            ))));
        }

        let entry = self.active_pages.get(&page_id);
        if let Some(entry) = entry {
            let frame = entry.value();
            Ok(frame.pin())
        } else {
            let buf = self.eviction_pages.evict();
            let pin_guard = buf.pin();
            {
                let mut lock_guard = pin_guard.lock().await;
                if lock_guard.is_dirty {
                    self.flush_page(&lock_guard.page).await?;
                }

                self.read_page(page_id, &mut lock_guard.page).await?;
                lock_guard.is_dirty = false;
                lock_guard.page_id = page_id;
            }
            self.active_pages.insert(page_id, buf);
            Ok(pin_guard)
        }
    }

    async fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<()> {
        let file = self.env.open_file(self.file_path.as_path()).await?;
        let pos = page_id as u64 * PAGE_SIZE as u64;
        match file.read_exact_at(page.raw_data_mut(), pos).await {
            Err(e) => Err(FloppyError::Io(e)),
            Ok(_) => Ok(()),
        }
    }
}
