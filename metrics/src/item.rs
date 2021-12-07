use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::MetricType;

pub(crate) trait ItemWriter {
    fn write(&mut self, name: &str, key: &str, sub_key: &str, val: f64);
}

use lazy_static::lazy_static;

lazy_static! {
    pub(crate) static ref EMPTY_ITEM: Arc<Item> = Arc::new(Item::new(ItemInner::new(
        String::new().into(),
        "",
        MetricType::Empty,
    )));
}

#[derive(Debug, Clone)]
pub struct ItemInner {
    pub(crate) name: Arc<String>,
    key: &'static str,
    t: MetricType,
}
impl ItemInner {
    pub(crate) fn new(name: Arc<String>, key: &'static str, t: MetricType) -> Self {
        Self { name, key, t }
    }
}

pub struct Item {
    inner: ItemInner,
    data: ItemData,
}
impl Item {
    pub(crate) fn new(inner: ItemInner) -> Self {
        let data = ItemData::new();
        Item { inner, data }
    }
    #[inline]
    pub(crate) fn empty() -> Arc<Self> {
        EMPTY_ITEM.clone()
    }
    #[inline(always)]
    pub(crate) fn inited(&self) -> bool {
        self.inner.t as u8 != MetricType::Empty as u8
    }
    #[inline(always)]
    pub(crate) fn incr(&self, c: usize) {
        debug_assert_ne!(self.inner.t as u8, MetricType::Empty as u8);
        self.data.cur.fetch_add(c, Ordering::Relaxed);
    }
    #[inline(always)]
    pub(crate) fn decr(&self, c: usize) {
        debug_assert_ne!(self.inner.t as u8, MetricType::Empty as u8);
        self.data.cur.fetch_sub(c, Ordering::Relaxed);
    }
    #[inline(always)]
    pub(crate) fn with_snapshot<F: Fn(&str, &'static str, f64)>(&self, secs: f64, visit: F) {
        let cur = self.data.cur.load(Ordering::Relaxed);
        let last = self.data.last.load(Ordering::Relaxed) as f64;
        self.data.last.store(cur, Ordering::Relaxed);
        let v = (cur as f64 - last) / secs;
        visit(&**self.inner.name, self.inner.key, v);
    }
}

struct ItemData {
    cur: AtomicUsize,
    last: AtomicUsize,
}
impl ItemData {
    fn new() -> Self {
        Self {
            cur: AtomicUsize::new(0),
            last: AtomicUsize::new(0),
        }
    }
}
