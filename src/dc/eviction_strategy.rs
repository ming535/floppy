use crate::dc::buf_frame::BufferFrameRef;

pub(crate) struct EvictionPool {}

impl EvictionPool {
    pub fn new(pool_size: usize) -> EvictionPool {
        Self {}
    }

    /// Makes the page held by `BufferFrame` a candidate for eviction.
    pub fn insert(&self, frame: BufferFrameRef) {}

    /// Ensures the page held by `BufferFrame` is no longer a candidate for eviction.
    pub fn delete(&self, frame: BufferFrameRef) {}

    /// Evicts a page previously marked as a candidate for eviction (if any),
    /// following the LRU eviction strategy.
    /// `BufferFrameManager` can use this frame after call this method.
    pub fn evict(&self) -> BufferFrameRef {
        todo!()
    }
}
