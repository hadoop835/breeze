mod bit_map;
mod cid;
mod cow;
mod offset;
mod ring;
mod slice;

pub use bit_map::BitMap;
pub use cid::*;
pub use cow::*;
pub use offset::*;
pub use ring::{ResizedRingBuffer, RingBuffer, RingSlice};
pub use slice::*;

macro_rules! define_read_number {
    ($($fn_name:ident, $type_name:tt);+) => {
        pub trait Buffer {
            fn write<D: AsRef<[u8]>>(&mut self, data: D);
            $(
            fn $fn_name(&mut self, num:$type_name);
            )+
        }

        impl Buffer for Vec<u8> {
            #[inline]
            fn write<D: AsRef<[u8]>>(&mut self, data: D) {
                let b = data.as_ref();
                use std::ptr::copy_nonoverlapping as copy;
                self.reserve(b.len());
                unsafe {
                    copy(
                        b.as_ptr() as *const u8,
                        self.as_mut_ptr().offset(self.len() as isize),
                        b.len(),
                    );
                    self.set_len(self.len() + b.len());
                }
            }
        $(
            #[inline(always)]
            fn $fn_name(&mut self, num: $type_name) {
                self.write(num.to_be_bytes());
            }
            )+
        }
    };
}

// big endian
define_read_number!(
    write_u8, u8;
    write_u16, u16;
    write_u32, u32;
    write_u64, u64
);
