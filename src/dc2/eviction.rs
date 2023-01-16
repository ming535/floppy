use crate::dc2::buf::Buffer;

pub(crate) struct EvictionPool {}

impl EvictionPool {
    pub fn new(_pool_size: usize) -> EvictionPool {
        Self {}
    }

    /// Makes the page held by `BufferFrame` a candidate for eviction.
    pub fn insert(&self, _frame: Buffer) {}

    /// Ensures the page held by `BufferFrame` is no longer a candidate for
    /// eviction.
    pub fn delete(&self, _frame: Buffer) {}

    /// Evicts a page previously marked as a candidate for eviction (if any),
    /// following the LRU eviction strategy.
    /// [`BufMgr`] can use this frame after call this method.
    pub fn evict(&self) -> Buffer {
        todo!()
    }
}
