mod buf_mgr;
mod codec;
mod lru;
mod page;
mod page_wal;
/// DC (Data Component)
mod tree;
mod tree_node;

const MAX_KEY_SIZE: usize = u16::MAX as usize;
const MAX_VALUE_SIZE: usize = u16::MAX as usize;
