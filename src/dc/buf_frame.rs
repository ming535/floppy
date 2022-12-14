use crate::dc::{
    node::NodeType,
    page::{PageId, PagePtr},
};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex, MutexGuard,
};

pub(crate) struct BufferFrame {
    page_id: PageId,
    page_ptr: PagePtr,
    fix_count: AtomicI64,
    dirty: bool,
}

pub(crate) type BufferFrameRef = Arc<Mutex<BufferFrame>>;

pub(crate) struct BufferFrameGuard<'a> {
    pub(crate) frame: Arc<Mutex<BufferFrame>>,
    pub(crate) guard: MutexGuard<'a, BufferFrame>,
}

impl BufferFrame {
    pub fn new(page_id: PageId, page_ptr: PagePtr) -> Self {
        Self {
            page_id,
            page_ptr,
            fix_count: AtomicI64::new(0),
            dirty: false,
        }
    }

    pub fn init(&mut self) {}

    pub fn page_ptr(&self) -> &PagePtr {
        &self.page_ptr
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn page_lsn(&self) -> u64 {
        let data = self.page_ptr.data();
        u64::from_le_bytes(data[0..8].try_into().unwrap())
    }

    pub fn node_type(&self) -> NodeType {
        let data = self.page_ptr.data();
        u8::from_le_bytes(data[8..9].try_into().unwrap()).into()
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        let data = self.page_ptr.data_mut();
        let type_flag: u8 = node_type.into();
        data[8..9].copy_from_slice(&type_flag.to_le_bytes());
    }

    pub fn payload<'a>(&self) -> &'a [u8] {
        &self.page_ptr.data()[9..]
    }

    pub fn payload_mut<'a>(&mut self) -> &'a mut [u8] {
        &mut self.page_ptr.data_mut()[9..]
    }

    pub fn fix(&self) -> i64 {
        self.fix_count.fetch_add(1, Ordering::Release)
    }

    pub fn unfix(&self) -> i64 {
        self.fix_count.fetch_add(-1, Ordering::Release)
    }
}
