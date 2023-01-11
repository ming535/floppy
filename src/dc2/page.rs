use crate::common::error::{FloppyError, Result};
use paste::paste;
use std::{
    alloc::{alloc, Layout},
    mem, ptr, slice,
};

/// Floppy's disk page is very similar to postgres. A page is a
/// slotted page of the form:
///  * +----------------+---------------------------------+
///  * | PageHeaderData | linp1 linp2 linp3 ...           |
///  * +-----------+----+---------------------------------+
///  * | ... linpN |                                      |
///  * +-----------+--------------------------------------+
///  * |           ^ pd_lower                             |
///  * |                                                  |
///  * |             v pd_upper                           |
///  * +-------------+------------------------------------+
///  * |             | tupleN ...                         |
///  * +-------------+------------------+-----------------+
///  * |       ... tuple3 tuple2 tuple1 | "special space" |
///  * +--------------------------------+-----------------+
///  *                                  ^ pd_special
///
type PageLsn = u64;

type PageChecksum = u16;

/// PageFlags is is not used right now.
type PageFlags = u8;

/// LocationIndex is the byte offset within a page.
type LocationIndex = u16;

pub(crate) struct PagePtr {
    buf: ptr::NonNull<u8>,
    size: usize,
    inited: bool,
}

macro_rules! access_header_data {
    ($name:ident, $t:ty) => {
        paste! {
            pub fn [<get $name>](&self) -> $t {
                let offset = self.[<$name _offset>]();
                let data = self.data();
                $t::from_le_bytes(
                    data[offset..offset + mem::size_of::<$t>()]
                        .try_into()
                        .unwrap(),
                )
            }
            pub fn [<set $name>](&mut self, v: $t) {
                let offset = self.[<$name _offset>]();
                let data = self.data_mut();
                data[offset..offset + mem::size_of::<$t>()].copy_from_slice(&v.to_le_bytes());
            }
        }
    };
}

impl PagePtr {
    pub fn alloc(size: usize) -> Result<Self> {
        let layout = Layout::from_size_align(size, mem::size_of::<usize>())?;
        unsafe {
            let buf = alloc(layout);
            if buf.is_null() {
                return Err(FloppyError::External(
                    "alloc mem failed".to_string(),
                ));
            }
            let buf = ptr::NonNull::new_unchecked(buf);
            Ok(Self {
                buf,
                size,
                inited: false,
            })
        }
    }

    pub fn init(&mut self, special_size: usize) {
        unsafe { ptr::write_bytes(self.buf.as_ptr(), 0, self.size) }
    }

    access_header_data!(lsn, PageLsn);
    access_header_data!(checksum, PageChecksum);
    access_header_data!(flags, PageFlags);
    access_header_data!(lower, LocationIndex);
    access_header_data!(upper, LocationIndex);
    access_header_data!(special, LocationIndex);

    fn data(&self) -> &[u8] {
        assert!(self.inited);
        unsafe { slice::from_raw_parts(self.buf.as_ptr(), self.size) }
    }

    fn data_mut(&mut self) -> &mut [u8] {
        assert!(self.inited);
        unsafe { slice::from_raw_parts_mut(self.buf.as_ptr(), self.size) }
    }

    #[inline]
    fn lsn_offset(&self) -> usize {
        0
    }

    #[inline]
    fn checksum_offset(&self) -> usize {
        self.lsn_offset() + mem::size_of::<PageLsn>()
    }

    #[inline]
    fn flags_offset(&self) -> usize {
        self.checksum_offset() + mem::size_of::<PageChecksum>()
    }

    #[inline]
    fn lower_offset(&self) -> usize {
        self.flags_offset() + mem::size_of::<PageFlags>()
    }

    #[inline]
    fn upper_offset(&self) -> usize {
        self.lower_offset() + mem::size_of::<LocationIndex>()
    }

    #[inline]
    fn special_offset(&self) -> usize {
        self.upper_offset() + mem::size_of::<LocationIndex>()
    }
}
