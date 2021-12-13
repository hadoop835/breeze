use std::sync::Arc;

use crate::{Id, ItemWriter, MetricType};

#[derive(Default, Debug)]
pub struct ItemData {
    id: Arc<Id>,
    inner: InnerData,
}

impl ItemData {
    #[inline(always)]
    pub(crate) fn init_id(&mut self, id: Arc<Id>) {
        debug_assert!(!self.id.valid());
        self.id = id;
    }
    #[inline(always)]
    pub(crate) fn snapshot<W: ItemWriter>(&self, w: &mut W, secs: f64) {
        use MetricType::*;
        unsafe {
            match self.id.t {
                Qps => self.inner.qps.snapshot(&*self.id, w, secs),
                Count => self.inner.number.snapshot(&*self.id, w, secs),
                Status => self.inner.status.snapshot(&*self.id, w, secs),
                RTT => self.inner.rtt.snapshot(&*self.id, w, secs),
                Empty => panic!("metric type empty, not inited"),
            }
        }
    }
    #[inline(always)]
    pub(crate) fn incr_num(&self, num: i64) {
        debug_assert!(self.id.t.is_num());
        unsafe { self.inner.number.incr(num) };
    }
}

use super::{Number, Qps, Rtt, StatusData};
use std::mem::ManuallyDrop;
union InnerData {
    empty: [u8; 48], // CacheLineSize. 一个Item是一个CacheLine
    number: ManuallyDrop<Number>,
    qps: ManuallyDrop<Qps>,
    status: ManuallyDrop<StatusData>,
    rtt: ManuallyDrop<Rtt>,
}
impl Default for InnerData {
    #[inline(always)]
    fn default() -> Self {
        Self { empty: [0u8; 48] }
    }
}
use std::fmt::{self, Debug, Formatter};
impl Debug for InnerData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "inner data(*) ")
    }
}
pub trait MetricData {
    fn incr_to(self, data: &ItemData);
    fn decr_to(self, data: &ItemData);
}
use crate::ToNumber;
impl<T: ToNumber> MetricData for T {
    #[inline(always)]
    fn incr_to(self, data: &ItemData) {
        data.incr_num(self.int());
    }
    #[inline(always)]
    fn decr_to(self, data: &ItemData) {
        data.incr_num(self.int() * -1);
    }
}
use std::time::Duration;
impl MetricData for Duration {
    #[inline(always)]
    fn incr_to(self, data: &ItemData) {
        unsafe { data.inner.rtt.incr(self) };
    }
    #[inline(always)]
    fn decr_to(self, _data: &ItemData) {
        debug_assert!(0 == 1);
    }
}
