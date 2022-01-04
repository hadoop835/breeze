use ds::RingSlice;

// redis分隔符是\r\n
pub(crate) const REDIS_SPLIT_LEN: usize = 2;

#[derive(Debug, Clone)]
pub struct Bulk {
    // meta除了$之外的起始位置
    pub meta_start: usize,
    // meta 结束位置，包括\r\n
    pub meta_end: usize,
    // data 结束位置，包括\r\n，如果data_end等于meta_end，说明是 $-1\r\n
    pub data_end: usize,
}

impl Bulk {
    pub fn from(bulk_start: usize, meta_end_offset: usize, data_end_offset: usize) -> Self {
        debug_assert!(
            meta_end_offset == data_end_offset || (data_end_offset - 2) > meta_end_offset
        );
        Self {
            meta_start: bulk_start,
            meta_end: bulk_start + meta_end_offset,
            data_end: bulk_start + data_end_offset,
        }
    }

    // 包括meta + bare_data + \r\n,like：$2\r\n12\r\n
    pub fn bulk_data(&self, origin: &RingSlice) -> RingSlice {
        origin.sub_slice(self.meta_start, self.data_end - self.meta_start + 1)
    }

    // 裸数据，即真正的data，不包括meta，不包含\r\n
    pub fn bare_data(&self, origin: &RingSlice) -> RingSlice {
        origin.sub_slice(self.meta_end + 1, self.data_end - self.meta_end - 2)
    }

    pub fn is_empty(&self) -> bool {
        self.data_end > self.meta_end
    }

    // pub fn end_pos(&self) -> usize {
    //     self.data_end
    // }
}
