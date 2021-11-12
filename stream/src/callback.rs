use std::mem::MaybeUninit;
use std::sync::{
    atomic::{AtomicBool, AtomicPtr, Ordering},
    Arc,
};

use atomic_waker::AtomicWaker;

use protocol::{Command, Error, HashedCommand};

#[derive(Clone)]
pub struct RequestCallback {
    done: Arc<AtomicBool>,
    data: Arc<AtomicPtr<MaybeUninit<(HashedCommand, Option<Command>)>>>,
    waker: Arc<AtomicWaker>,
}
impl RequestCallback {
    #[inline(always)]
    pub fn new(waker: Arc<AtomicWaker>) -> Self {
        Self {
            done: Default::default(),
            data: Default::default(),
            waker,
        }
    }
}

impl RequestCallback {
    #[inline(always)]
    fn _on_complete(self, req: HashedCommand, resp: Option<Command>) {
        debug_assert!(!self.complete());
        debug_assert!(self.data.load(Ordering::Acquire).is_null());
        let mut d = MaybeUninit::new((req, resp));
        self.data.store(&mut d, Ordering::Release);
        self.done.store(true, Ordering::Release);
        self.waker.wake();
    }
    #[inline(always)]
    pub(crate) fn on_complete(self, req: HashedCommand, resp: Command) {
        self._on_complete(req, Some(resp));
    }
    #[inline(always)]
    pub(crate) fn on_err(self, req: HashedCommand, _err: Error) {
        self._on_complete(req, None);
    }
    #[inline(always)]
    pub(crate) fn take(&self) -> (HashedCommand, Option<Command>) {
        debug_assert!(self.complete());
        let data = self.data.load(Ordering::Acquire);
        debug_assert!(!data.is_null());
        // double free
        let d = unsafe { data.read().assume_init() };
        // debug环境下，把原有指针设置为0，每次take之前进行null判断。避免double free.
        debug_assert!(!self.data.swap(0 as *mut _, Ordering::Release).is_null());
        d
    }
    #[inline(always)]
    pub(crate) fn complete(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

unsafe impl Send for RequestCallback {}
unsafe impl Sync for RequestCallback {}
