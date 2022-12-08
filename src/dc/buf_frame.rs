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

    pub fn get_page_lsn(&self) -> u64 {
        let data = self.page_ptr.data();
        u64::from_le_bytes(data[0..8].try_into().unwrap())
    }

    pub fn get_page_type(&self) -> u8 {
        let data = self.page_ptr.data();
        u8::from_le_bytes(data[8..9].try_into().unwrap())
    }

    pub fn payload<'a>(&self) -> &'a [u8] {
        &self.page_ptr.data()[9..]
    }

    pub fn payload_mut<'a>(&mut self) -> &'a mut [u8] {
        &mut self.page_ptr.data_mut()[9..]
    }
}
