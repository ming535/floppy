use crate::dc2::{
    opaque::opaque_data_accessor,
    page::{Page, PageId},
};
use paste::paste;
use std::mem;

/// [`MetaPage`] is the first page of file, its [`PageId`]
/// is zero. It contains important information like
/// the root of a tree, the header of a list of free pages.
pub(super) struct MetaPage<'a> {
    page: &'a mut Page,
}

impl<'a> MetaPage<'a> {
    pub fn from_page(page: &'a mut Page) -> Self {
        Self { page }
    }

    fn opaque_size() -> usize {
        mem::size_of::<PageId>()
    }

    opaque_data_accessor!(root, PageId);

    fn root_offset(&self) -> usize {
        0
    }
}
