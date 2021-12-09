#[derive(Clone)]
pub(crate) struct TimeoutWaker {
    id: usize,
}

impl TimeoutWaker {
    //#[inline(always)]
    //pub(crate) fn register(&self, waker: &Waker) {}
    //#[inline(always)]
    //pub(crate) fn unregister(&self) {}
}

//use std::task::Waker;
//pub(crate) fn build_waker() -> TimeoutWaker {
//    TimeoutWaker { id: 0 }
//}
