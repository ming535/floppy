use std::{mem, slice};
/// Allows an object to be encoded and decoded.
pub(crate) trait Codec {
    /// Returns the exact size to encode the object.
    fn encode_size(&self) -> usize;

    /// Encodes the object to the encoder.
    ///
    /// # Safety
    ///
    /// The encoder must have enough space to encode the object.
    unsafe fn encode_to(&self, encoder: &mut Encoder);

    /// Decodes an object from the decoder.
    ///
    /// # Safety
    ///
    ///   The decoder must have enough data to decode the object.
    unsafe fn decode_from(decoder: &mut Decoder) -> Self;
}

/// An unsafe, big-endian encoder.
pub(crate) struct Encoder {
    buf: *mut u8,
    len: usize,
    cursor: *mut u8,
}

macro_rules! put_int {
    ($name:ident, $t:ty) => {
        pub unsafe fn $name(&mut self, v: $t) {
            let v = v.to_le();
            let ptr = &v as *const $t as *const u8;
            let len = mem::size_of::<$t>();
            self.take(len).copy_from_nonoverlapping(ptr, len);
        }
    };
}

impl Encoder {
    pub fn new(buf: &mut [u8]) -> Self {
        Self {
            buf: buf.as_mut_ptr(),
            len: buf.len(),
            cursor: buf.as_mut_ptr(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub unsafe fn offset(&self) -> usize {
        self.cursor.offset_from(self.buf) as usize
    }

    pub unsafe fn remaining(&self) -> usize {
        self.len() - self.offset()
    }

    unsafe fn take(&mut self, n: usize) -> *mut u8 {
        assert!(n <= self.remaining());
        let ptr = self.cursor;
        self.cursor = self.cursor.add(n);
        ptr
    }

    put_int!(put_u8, u8);
    put_int!(put_u16, u16);
    put_int!(put_u32, u32);
    put_int!(put_u64, u64);

    pub unsafe fn put_byte_slice(&mut self, v: &[u8]) {
        let cursor = self.take(v.len());
        cursor.copy_from_nonoverlapping(v.as_ptr(), v.len());
    }
}

/// An unsafe, big-endian decoder.
pub(crate) struct Decoder {
    buf: *const u8,
    len: usize,
    cursor: *const u8,
}

macro_rules! get_int {
    ($name:ident, $t:ty) => {
        pub unsafe fn $name(&mut self) -> $t {
            let mut v: $t = 0;
            let ptr = &mut v as *mut $t as *mut u8;
            let len = mem::size_of::<$t>();
            self.take(len).copy_to_nonoverlapping(ptr, len);
            <$t>::from_le(v)
        }
    };
}

impl Decoder {
    pub fn new(buf: &[u8]) -> Self {
        Self {
            buf: buf.as_ptr(),
            len: buf.len(),
            cursor: buf.as_ptr(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub unsafe fn offset(&self) -> usize {
        self.cursor.offset_from(self.buf) as usize
    }

    pub unsafe fn remaining(&self) -> usize {
        self.len() - self.offset()
    }

    unsafe fn take(&mut self, n: usize) -> *const u8 {
        assert!(n <= self.remaining());
        let ptr = self.cursor;
        self.cursor = self.cursor.add(n);
        ptr
    }

    get_int!(get_u8, u8);
    get_int!(get_u16, u16);
    get_int!(get_u32, u32);
    get_int!(get_u64, u64);

    pub unsafe fn get_byte_slice<'a>(&mut self, len: usize) -> &'a [u8] {
        let cursor = self.take(len);
        slice::from_raw_parts(cursor, len)
    }
}
