use std::sync::atomic::{
    AtomicBool, AtomicU32, AtomicUsize,
    Ordering::{self, *},
};
use std::sync::Arc;

use crate::{Id, MetricType};

pub(crate) trait ItemWriter {
    fn write(&mut self, name: &str, key: &str, sub_key: &str, val: f64);
}

use lazy_static::lazy_static;

lazy_static! {
    pub(crate) static ref EMPTY_ITEM: Arc<Item> = Default::default();
}

unsafe impl Send for Item {}
unsafe impl Sync for Item {}

pub struct ItemRc {
    inner: *const Item,
}
impl ItemRc {
    #[inline(always)]
    pub fn uninit() -> ItemRc {
        Self {
            inner: 0 as *const _,
        }
    }
    #[inline(always)]
    pub fn inited(&self) -> bool {
        !self.inner.is_null()
    }
    #[inline(always)]
    pub fn try_init(&mut self, idx: usize) {
        if let Some(item) = crate::get_metric(idx) {
            debug_assert!(!item.is_null());
            self.inner = item;
            self.incr_rc();
        }
    }
}
use std::ops::Deref;
impl Deref for ItemRc {
    type Target = Item;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        debug_assert!(self.inited());
        unsafe { &*self.inner }
    }
}
impl Drop for ItemRc {
    #[inline]
    fn drop(&mut self) {
        if self.inited() {
            self.decr_rc();
        }
    }
}

#[derive(Debug, Default)]
pub struct Item {
    lock: AtomicBool,
    pub(crate) rc: AtomicU32,
    pub(crate) id: Arc<Id>,
    data: ItemData,
}
impl Item {
    #[inline]
    pub(crate) fn init(&mut self, id: Arc<Id>) {
        assert_eq!(self.rc(), 0);
        self.id = id;
        self.incr_rc();
    }
    #[inline]
    pub(crate) fn inited(&self) -> bool {
        self.rc() > 0
    }
    #[inline(always)]
    pub(crate) fn incr(&self, c: usize) {
        debug_assert_ne!(self.id.t as u8, MetricType::Empty as u8);
        self.data.cur.fetch_add(c, Ordering::Relaxed);
    }
    #[inline(always)]
    pub(crate) fn decr(&self, c: usize) {
        debug_assert_ne!(self.id.t as u8, MetricType::Empty as u8);
        self.data.cur.fetch_sub(c, Ordering::Relaxed);
    }
    #[inline(always)]
    pub(crate) fn with_snapshot<W: crate::ItemWriter>(&self, w: &mut W, secs: f64) {
        let cur = self.data.cur.load(Ordering::Relaxed);
        let last = self.data.last.load(Ordering::Relaxed) as f64;
        self.data.last.store(cur, Ordering::Relaxed);
        let v = (cur as f64 - last) / secs;
        let sub_key = "qps";
        w.write(&*self.id.path, self.id.key, sub_key, v);
    }
    #[inline]
    fn rc(&self) -> usize {
        self.rc.load(Ordering::Acquire) as usize
    }
    #[inline]
    fn incr_rc(&self) -> usize {
        self.rc.fetch_add(1, Ordering::AcqRel) as usize
    }
    #[inline]
    fn decr_rc(&self) -> usize {
        self.rc.fetch_sub(1, Ordering::AcqRel) as usize
    }
    // 没有任何引用，才能够获取其mut
    pub(crate) fn try_lock<'a>(&self) -> Option<ItemWriteGuard<'a>> {
        self.lock
            .compare_exchange(false, true, AcqRel, Relaxed)
            .map(|_| ItemWriteGuard {
                item: unsafe { &mut *(self as *const _ as *mut _) },
            })
            .ok()
    }
    #[inline]
    fn unlock(&self) {
        self.lock
            .compare_exchange(true, false, AcqRel, Relaxed)
            .expect("unlock failed");
    }
}

pub struct ItemWriteGuard<'a> {
    item: &'a mut Item,
}
impl<'a> Deref for ItemWriteGuard<'a> {
    type Target = Item;
    fn deref(&self) -> &Self::Target {
        &self.item
    }
}
use std::ops::DerefMut;
impl<'a> DerefMut for ItemWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.item
    }
}
impl<'a> Drop for ItemWriteGuard<'a> {
    fn drop(&mut self) {
        self.unlock();
    }
}

#[derive(Debug, Default)]
struct ItemData {
    cur: AtomicUsize,
    last: AtomicUsize,
}
