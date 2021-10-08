use super::RingSlice;

use std::ptr::NonNull;
use std::slice::from_raw_parts_mut;

// <= read的字节是已经全部读取完的
// [read, processed]是已经写入，但未全部收到读取完成通知的
// write是当前写入的地址
pub struct RingBuffer {
    data: NonNull<u8>,
    size: usize,
    read: usize,
    processed: usize,
    write: usize,
}

impl RingBuffer {
    pub fn with_capacity(size: usize) -> Self {
        let buff_size = size.next_power_of_two();
        let mut data = Vec::with_capacity(buff_size);
        let ptr = unsafe { NonNull::new_unchecked(data.as_mut_ptr()) };
        std::mem::forget(data);
        Self {
            size: buff_size,
            data: ptr,
            read: 0,
            processed: 0,
            write: 0,
        }
    }
    #[inline]
    pub fn read(&self) -> usize {
        self.read
    }
    #[inline(always)]
    pub fn processed(&self) -> usize {
        self.processed
    }
    #[inline(always)]
    pub fn advance_read(&mut self, n: usize) {
        self.read += n;
    }
    #[inline(always)]
    pub fn writtened(&self) -> usize {
        self.write
    }
    #[inline(always)]
    pub fn advance_processed(&mut self, n: usize) {
        self.processed += n;
    }
    #[inline(always)]
    pub fn advance_write(&mut self, n: usize) {
        self.write += n;
    }
    #[inline(always)]
    fn mask(&self, offset: usize) -> usize {
        offset & (self.size - 1)
    }
    // 返回可写入的buffer。如果无法写入，则返回一个长度为0的slice
    #[inline(always)]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        let offset = self.mask(self.write);
        let n = if self.read + self.size == self.write {
            // 已满
            0
        } else {
            let read = self.mask(self.read);
            if offset < read {
                read - offset
            } else {
                self.size - offset
            }
        };
        unsafe { from_raw_parts_mut(self.data.as_ptr().offset(offset as isize), n) }
    }
    #[inline]
    pub fn write(&mut self, data: &RingSlice) -> usize {
        let mut n = 0;
        for s in data.as_slices() {
            let src = s.data();
            let mut l = 0;
            while l < src.len() {
                let dst = self.as_mut_bytes();
                let c = (src.len() - l).min(dst.len());
                if c == 0 {
                    return n;
                }
                use std::ptr::copy_nonoverlapping as copy;
                unsafe { copy(src.as_ptr().offset(l as isize), dst.as_mut_ptr(), c) };
                self.advance_write(c);
                l += c;
                n += c;
            }
        }
        n
    }
    // 返回已写入，未处理的字节。即从[processed,write)的字节
    #[inline(always)]
    pub fn processing_bytes(&self) -> RingSlice {
        RingSlice::from(self.data.as_ptr(), self.size, self.processed, self.write)
    }
    // 返回可读取的数据。可能只返回部分数据。如果要返回所有的可读数据，使用 data 方法。
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        let offset = self.mask(self.read);
        let n = (self.cap() - offset).min(self.len());
        unsafe { from_raw_parts_mut(self.data.as_ptr().offset(offset as isize), n) }
    }
    // 返回已写入的所有数据，包括已处理未确认的
    #[inline(always)]
    pub fn data(&self) -> RingSlice {
        RingSlice::from(self.data.as_ptr(), self.size, self.read, self.write)
    }
    #[inline(always)]
    pub fn cap(&self) -> usize {
        self.size
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        debug_assert!(self.write >= self.read);
        self.write - self.read
    }
    #[inline(always)]
    pub(crate) fn available(&self) -> bool {
        self.read() + self.cap() > self.writtened()
    }
    #[inline(always)]
    pub(crate) fn avail(&self) -> usize {
        self.cap() - self.len()
    }
    #[inline(always)]
    pub fn ratio(&self) -> (usize, usize) {
        assert!(self.write >= self.read);
        (self.write - self.read, self.cap())
    }

    // cap > self.len()
    pub(crate) fn resize(&self, cap: usize) -> Self {
        let cap = cap.next_power_of_two();
        assert!(cap >= self.write - self.read);
        let mut new = Self::with_capacity(cap);
        new.read = self.read;
        new.processed = self.processed;
        new.write = self.read;
        let old_data = self.data();
        use std::ptr::copy_nonoverlapping as copy;
        for slice in old_data.as_slices() {
            let mut offset = 0usize;
            while offset < slice.len() {
                let dst = new.as_mut_bytes();
                // 向ring-buffer写入slice数据的时候，可能会分段。
                let count = dst.len().min(slice.len() - offset);
                unsafe {
                    copy(
                        slice.as_ptr().offset(offset as isize),
                        dst.as_mut_ptr(),
                        count,
                    );
                }
                new.advance_write(count);
                offset += count;
            }
        }
        assert_eq!(self.write, new.write);
        assert_eq!(self.processed, new.processed);
        assert_eq!(self.read, new.read);
        new
    }
    #[inline(always)]
    pub fn reset(&mut self) {
        self.read = 0;
        self.write = 0;
        self.processed = 0;
    }
}

impl Drop for RingBuffer {
    fn drop(&mut self) {
        unsafe {
            let _ = Vec::from_raw_parts(self.data.as_ptr(), 0, self.size);
        }
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for RingBuffer {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rb:(cap:{} read:{} processed:{} write:{})",
            self.size, self.read, self.processed, self.write
        )
    }
}
impl Debug for RingBuffer {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rb:(ptr:{:#x} cap:{} read:{} processed:{} write:{})",
            self.data.as_ptr() as usize,
            self.size,
            self.read,
            self.processed,
            self.write
        )
    }
}

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}
