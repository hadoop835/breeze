use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use discovery::{TopologyRead, TopologyReadGuard};
use protocol::{Endpoint, Topology};
use sharding::hash::Hasher;

// 支持刷新
pub struct RefreshTopology<T> {
    last: usize,  // 上一次刷时的cycle
    ticks: usize, // 调用try_refresh的次数。
    ep: NonNull<T>,
    reader: TopologyReadGuard<T>,
}
impl<T: Clone> RefreshTopology<T> {
    #[inline]
    pub fn new(reader: TopologyReadGuard<T>) -> Self {
        let last = reader.cycle();
        let ep = Box::leak(Box::new(reader.do_with(|t| t.clone())));
        let ep = unsafe { NonNull::new_unchecked(ep) };
        Self {
            last,
            ep,
            reader,
            ticks: 0,
        }
    }
    #[inline]
    fn top(&self) -> &T {
        unsafe { self.ep.as_ref() }
    }
}
impl<T: Endpoint + Clone> Endpoint for RefreshTopology<T> {
    type Item = T::Item;
    #[inline(always)]
    fn send(&self, req: T::Item) {
        self.top().send(req);
    }
}
impl<T: Topology + Clone> Topology for RefreshTopology<T> {
    #[inline(always)]
    fn hasher(&self) -> &Hasher {
        self.top().hasher()
    }
}

unsafe impl<T> Send for RefreshTopology<T> {}
unsafe impl<T> Sync for RefreshTopology<T> {}
