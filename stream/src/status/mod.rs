use crate::AtomicWaker;
use std::io::{Error, ErrorKind, Result};
use std::task::{Context, Poll};

use ds::{Lock, LockGuard};
use protocol::{Request, RequestId, Response};

mod inner;
use inner::*;
mod data;
use data::*;

#[derive(Default)]
pub struct Status {
    inner: Lock<StatusInner>,
    waker: AtomicWaker,
}

impl Status {
    pub fn new(cid: usize) -> Self {
        Self {
            inner: Lock::new(StatusInner::new(cid)),
            ..Default::default()
        }
    }
    #[inline(always)]
    fn lock(&self) -> LockGuard<StatusInner> {
        self.inner.lock()
    }
    #[inline(always)]
    fn try_lock(&self) -> Result<LockGuard<StatusInner>> {
        self.inner
            .try_lock()
            .ok_or(Error::new(ErrorKind::Other, "lock failed"))
    }
    // 只尝试获取一次锁。如果锁获取失败，说明状态一定有问题。
    #[inline(always)]
    pub(crate) fn place_request(&self, req: &Request) -> Result<()> {
        self.lock().place_request(req)
    }
    // 如果状态不是RequestReceived, 则返回None.
    // 有可能是连接被重置导致。
    #[inline(always)]
    pub(crate) fn take_request(&self) -> Result<(usize, Request)> {
        self.lock().take_request()
    }

    // 获取锁失败或者未获取到response，都进入pending状态。
    #[inline(always)]
    pub(crate) fn poll_read(&self, cx: &mut Context) -> Poll<Result<(RequestId, Response)>> {
        if let Ok(mut data) = self.try_lock() {
            if let Some(r) = data.take_response()? {
                return Poll::Ready(Ok(r));
            }
        }
        // 未获取到锁，或者当前数据未就绪
        self.waker.register(cx.waker());
        Poll::Pending
    }
    #[inline(always)]
    pub fn place_response(&self, response: protocol::Response) -> Result<()> {
        self.lock().place_response(response)?;
        self.waker.wake();
        Ok(())
    }
    // 在在sender把Response发送给client后，在Drop中会调用response_done，更新状态。
    #[inline(always)]
    pub fn response_done(&self) -> Result<()> {
        self.lock().response_done()
    }
    // reset只会把状态从shutdown变更为init
    // 必须在done设置为true之后调用。否则会有data race
    pub(crate) fn reset(&self) -> Result<()> {
        self.lock().reset();
        Ok(())
    }
}
unsafe impl Send for Status {}
