use crate::dc2::page::{Page, PageId};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};
use tokio::sync::{Mutex, MutexGuard};

pub(crate) struct PinGuard {
    // todo should we use reference?
    buf: Buffer,
}

impl PinGuard {
    pub fn new(buf: Buffer) -> Self {
        buf.inner.pin_count.fetch_add(1, Ordering::Release);
        Self { buf }
    }

    pub async fn lock(&self) -> MutexGuard<BufferState> {
        self.buf.inner.state.lock().await
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.buf.inner.pin_count.fetch_add(-1, Ordering::Release);
    }
}

#[derive(Clone)]
pub(crate) struct Buffer {
    inner: Arc<BufferInner>,
}

impl Buffer {
    pub fn new(page_id: PageId, page: Page) -> Self {
        let inner = BufferInner {
            pin_count: AtomicI64::new(0),
            state: Mutex::new(BufferState {
                page_id,
                is_dirty: false,
                page,
            }),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn pin(&self) -> PinGuard {
        PinGuard::new(self.clone())
    }
}

/// Shared state for a buffer. Operations on this struct should
/// hold a lock.
pub(crate) struct BufferState {
    pub page_id: PageId,
    pub is_dirty: bool,
    pub page: Page,
}

struct BufferInner {
    pin_count: AtomicI64,
    state: Mutex<BufferState>,
}
