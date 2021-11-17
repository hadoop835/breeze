use std::mem::MaybeUninit;
use std::sync::{
    atomic::{AtomicPtr, AtomicU8, Ordering},
    Arc,
};

use atomic_waker::AtomicWaker;

use protocol::{Command, Error, HashedCommand};

pub struct RequestCallback {
    data: AtomicPtr<(HashedCommand, Option<Command>)>,
    waker: Arc<AtomicWaker>,
}
impl RequestCallback {
    #[inline(always)]
    pub fn new(waker: Arc<AtomicWaker>) -> Self {
        Self {
            data: Default::default(),
            waker,
        }
    }
}

impl RequestCallback {
    #[inline(always)]
    fn _on_complete(&self, req: HashedCommand, resp: Option<Command>) {
        debug_assert!(!self.complete());
        debug_assert!(self.data.load(Ordering::Acquire).is_null());
        let mut d = (req, resp);
        let empty = self.data.swap(&mut d, Ordering::Release);
        // 在take的时候释放。
        std::mem::forget(d);
        debug_assert!(empty.is_null());
        self.waker.wake();
    }
    #[inline(always)]
    pub(crate) fn on_complete(&self, req: HashedCommand, resp: Command) {
        self._on_complete(req, Some(resp));
    }
    #[inline(always)]
    pub(crate) fn on_err(&self, req: HashedCommand, _err: Error) {
        self._on_complete(req, None);
    }
    #[inline(always)]
    pub(crate) fn get(&self) -> &(HashedCommand, Option<Command>) {
        debug_assert!(self.complete());
        unsafe { &*self.data.load(Ordering::Acquire) }
    }
    // take只能调用一次，否则会可能会由于ptr::read()触发double free.
    #[inline(always)]
    pub(crate) fn take(&self) -> (HashedCommand, Option<Command>) {
        debug_assert!(self.complete());
        let data = self.data.swap(0 as *mut _, Ordering::Release);
        debug_assert!(!data.is_null());
        unsafe { data.read() }
    }
    #[inline(always)]
    pub(crate) fn complete(&self) -> bool {
        !self.data.load(Ordering::Acquire).is_null()
    }
}

unsafe impl Send for RequestCallback {}
unsafe impl Sync for RequestCallback {}

impl Drop for RequestCallback {
    fn drop(&mut self) {
        let data = *self.data.get_mut();
        log::info!("request call back dropped:{}", data.is_null());
        debug_assert!(data.is_null());
    }
}
