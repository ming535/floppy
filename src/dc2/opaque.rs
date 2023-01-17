/// opaque_data_accessor defines setter and getter method used
/// to access fields in opaque data in [`Page`].
/// It assumes every fields have a "offset" method defines its
/// offset in bytes related to the start of the opaque area.
macro_rules! opaque_data_accessor {
    ($name:ident, $t:ty) => {
        paste! {
            #[inline(always)]
            pub fn [<get _ $name>](&self) -> $t {
                let offset = self.[<$name _offset>]();
                let opaque = self.page.opaque_data();
                $t::from_le_bytes(
                    opaque[offset..offset + mem::size_of::<$t>()]
                        .try_into()
                        .unwrap(),
                )
            }

            #[inline(always)]
            pub fn [<set _ $name>](&mut self, v: $t) {
                let offset = self.[<$name _offset>]();
                let opaque_mut = self.page.opaque_data_mut();
                opaque_mut[offset..offset + mem::size_of::<$t>()].copy_from_slice(v.to_le_bytes().as_slice());
            }
        }
    };
}

pub(crate) use opaque_data_accessor;
