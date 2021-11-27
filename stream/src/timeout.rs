#[derive(Clone)]
pub(crate) struct TimeoutWaker {
    id: usize,
}

use std::task::Waker;

impl TimeoutWaker {
    //#[inline(always)]
    //pub(crate) fn register(&self, waker: &Waker) {}
    //#[inline(always)]
    //pub(crate) fn unregister(&self) {}
}

pub(crate) fn build_waker() -> TimeoutWaker {
    TimeoutWaker { id: 0 }
}
