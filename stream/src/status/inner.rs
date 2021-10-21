use std::io::{Error, ErrorKind, Result};

use super::DataType;
use super::ItemStatus::{self, *};
use protocol::{Request, RequestId, Response};

#[derive(Default)]
pub(super) struct StatusInner {
    id: u16,
    status: u8, // 0: 待接收请求。
    rid: RequestId,
    data: Data,
}

impl StatusInner {
    pub(super) fn new(cid: usize) -> Self {
        Self {
            id: cid as u16,
            ..Default::default()
        }
    }
    #[inline(always)]
    pub(super) fn place_request(&mut self, req: &Request) -> Result<()> {
        self.rid = req.id();
        self.cas(Init, RequestReceived)?;
        self.data.place_request(req.clone());
        Ok(())
    }
    // 如果状态不是RequestReceived, 则返回None.
    // 有可能是连接被重置导致。
    #[inline(always)]
    pub(super) fn take_request(&mut self) -> Result<(usize, Request)> {
        log::debug!("take request. {} ", self);
        self.cas(RequestReceived, RequestSent)?;
        Ok((self.id as usize, self.data.take_request()))
    }

    #[inline(always)]
    pub(super) fn place_response(&mut self, response: protocol::Response) -> Result<()> {
        self.cas(RequestSent, ResponseReceived)?;
        log::debug!("place response:{:?} ", response.location());
        self.data.place_response(response);
        Ok(())
    }

    #[inline(always)]
    pub(super) fn take_response(&mut self) -> Result<Option<(RequestId, Response)>> {
        if self.try_cas(ResponseReceived, Read) {
            let response = self.data.take_response();
            log::debug!("take response {:?}", response.location());
            Ok(Some((self.rid.clone(), response)))
        } else {
            // 说明response还未返回。
            if self.status == RequestSent || self.status == RequestReceived {
                Ok(None)
            } else {
                log::warn!("not a valid status to take response:{}", self.status);
                Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("status not valid:{}", self),
                ))
            }
        }
    }
    // 在在sender把Response发送给client后，在Drop中会调用response_done，更新状态。
    #[inline(always)]
    pub(super) fn response_done(&mut self) -> Result<()> {
        self.cas(Read, Init)?;
        Ok(())
    }
    // reset只会把状态从shutdown变更为init
    // 必须在done设置为true之后调用。否则会有data race
    pub(super) fn reset(&mut self) {
        if self.status != Init {
            log::warn!("reset. {} ", self);
        }
        let status: ItemStatus = self.status.into();
        self.data.drop(status.into());
        self.status = Init as u8;
    }
    #[inline(always)]
    fn cas(&mut self, old: ItemStatus, new: ItemStatus) -> Result<()> {
        if self.try_cas(old, new) {
            Ok(())
        } else {
            return Err(Error::new(ErrorKind::Other, "status cas error"));
        }
    }
    #[inline(always)]
    fn try_cas(&mut self, old: ItemStatus, new: ItemStatus) -> bool {
        if self.status == old {
            self.status = new as u8;
            true
        } else {
            if old != self.status && old as u8 != ResponseReceived {
                log::info!(
                    "cas-{} :({}=>{}): {}",
                    old == self.status,
                    old as u8,
                    new as u8,
                    self
                );
            }
            false
        }
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for StatusInner {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "id:{} rid:{} status:{}", self.id, self.rid, self.status)
    }
}

use std::mem::ManuallyDrop;
#[repr(C)]
union Data {
    empty: u8,
    request: ManuallyDrop<Request>,
    response: ManuallyDrop<Response>,
}

impl Default for Data {
    #[inline(always)]
    fn default() -> Self {
        Data { empty: 0 }
    }
}

impl Data {
    #[inline(always)]
    fn place_request(&mut self, r: Request) {
        self.request = ManuallyDrop::new(r);
    }
    #[inline(always)]
    fn take_request(&mut self) -> Request {
        unsafe { ManuallyDrop::take(&mut self.request) }
    }
    #[inline(always)]
    fn place_response(&mut self, r: Response) {
        self.response = ManuallyDrop::new(r);
    }
    #[inline(always)]
    fn take_response(&mut self) -> Response {
        unsafe { ManuallyDrop::take(&mut self.response) }
    }
    #[inline(always)]
    fn drop(&mut self, t: DataType) {
        match t {
            DataType::Request => {
                let _ = self.take_request();
            }
            DataType::Response => {
                let _ = self.take_response();
            }
            _ => {}
        }
    }
}
