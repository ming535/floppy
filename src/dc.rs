mod buf_frame;
mod buf_mgr;
mod codec;
mod eviction_strategy;
mod node;
mod page;
mod page_wal;
mod slot_array;
/// DC (Data Component)
mod tree;

const MAX_KEY_SIZE: usize = u16::MAX as usize;
const MAX_VALUE_SIZE: usize = u16::MAX as usize;
