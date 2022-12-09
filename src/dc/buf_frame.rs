use crate::dc::node::NodeType;
use crate::dc::page::{PageId, PagePtr};

pub(crate) struct BufferFrame {
    page_id: PageId,
    page_ptr: PagePtr,
    pin_count: usize,
    dirty: bool,
}

impl BufferFrame {
    pub fn new(page_id: PageId, page_ptr: PagePtr) -> Self {
        Self {
            page_id: page_id,
            page_ptr,
            pin_count: 0,
            dirty: false,
        }
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
}
