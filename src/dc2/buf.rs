use crate::dc2::page::{Page, PageId};
use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone)]
pub(crate) struct BufferFrame {
    inner: Arc<Mutex<BufferFrameInner>>,
}

impl BufferFrame {
    pub fn new(page_id: PageId, page: Page) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BufferFrameInner::new(page_id, page))),
        }
    }

    pub async fn guard(
        &self,
        _parent_guard: Option<BufferFrameGuard>,
    ) -> BufferFrameGuard {
        let guard = self.inner.clone().lock_owned().await;
        guard.fix();
        BufferFrameGuard { guard }
    }
}

pub(crate) struct BufferFrameInner {
    page_id: PageId,
    page: Page,
    fix_count: AtomicI64,
    dirty: bool,
}

impl BufferFrameInner {
    pub fn new(page_id: PageId, page: Page) -> Self {
        Self {
            page_id,
            page,
            fix_count: AtomicI64::new(0),
            dirty: false,
        }
    }

    pub fn init(&mut self) {}

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.page
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn fix(&self) -> i64 {
        self.fix_count.fetch_add(1, Ordering::Release)
    }

    pub fn unfix(&self) -> i64 {
        self.fix_count.fetch_add(-1, Ordering::Release)
    }
}

pub(crate) struct BufferFrameGuard {
    guard: OwnedMutexGuard<BufferFrameInner>,
}

impl Deref for BufferFrameGuard {
    type Target = BufferFrameInner;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for BufferFrameGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl BufferFrameGuard {
    pub async fn new(frame: BufferFrame) -> Self {
        let guard = frame.inner.clone().lock_owned().await;
        guard.fix();
        Self { guard }
    }
}

impl Drop for BufferFrameGuard {
    fn drop(&mut self) {
        self.guard.unfix();
    }
}
