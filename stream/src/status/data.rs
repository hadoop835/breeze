#[repr(u8)]
#[derive(Clone, Copy)]
pub(super) enum ItemStatus {
    Init = 0u8,
    RequestReceived,
    RequestSent,
    ResponseReceived, // 数据已写入
    Read,             // 已读走
}
const STATUSES: [ItemStatus; 5] = [Init, RequestReceived, RequestSent, ResponseReceived, Read];
impl From<u8> for ItemStatus {
    #[inline(always)]
    fn from(s: u8) -> Self {
        STATUSES[s as usize]
    }
}
use ItemStatus::*;
impl PartialEq<u8> for ItemStatus {
    #[inline(always)]
    fn eq(&self, other: &u8) -> bool {
        *self as u8 == *other
    }
}
impl PartialEq<ItemStatus> for u8 {
    #[inline(always)]
    fn eq(&self, other: &ItemStatus) -> bool {
        *self == *other as u8
    }
}
impl Into<DataType> for ItemStatus {
    fn into(self) -> DataType {
        match self {
            RequestReceived => DataType::Request,
            ResponseReceived => DataType::Response,
            _ => DataType::Empty,
        }
    }
}

pub(super) enum DataType {
    Empty,
    Request,
    Response,
}

unsafe impl Send for ItemStatus {}
