use crate::dc2::page::{Page, PageId};
use std::{
    mem,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

#[derive(Clone)]
pub(crate) struct PinGuard(Arc<PinGuardInner>);

impl PinGuard {
    pub fn new(buf: Buffer) -> Self {
        PinGuard(Arc::new(PinGuardInner::new(buf)))
    }

    pub fn lock(&self) -> LockGuard {
        LockGuard {
            pin_guard: self.clone(),
            guard: unsafe {
                // transmute to a 'static Guard.
                mem::transmute(self.0.buf.inner.state.lock().unwrap())
            },
        }
    }
}

struct PinGuardInner {
    buf: Buffer,
}

impl PinGuardInner {
    fn new(buf: Buffer) -> Self {
        buf.inner.pin_count.fetch_add(1, Ordering::Release);
        Self { buf }
    }
}

impl Drop for PinGuardInner {
    fn drop(&mut self) {
        self.buf.inner.pin_count.fetch_add(-1, Ordering::Release);
    }
}

pub(crate) struct LockGuard {
    pin_guard: PinGuard,
    guard: MutexGuard<'static, BufferState>,
}

impl LockGuard {
    /// Consume the current [`LockGuard`], release the lock
    /// and returns a [`PinGuard`]
    pub fn unlock(self) -> PinGuard {
        self.pin_guard
    }
}

impl Deref for LockGuard {
    type Target = MutexGuard<'static, BufferState>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for LockGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
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
