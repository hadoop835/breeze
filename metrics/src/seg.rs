use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::MetricType;
use crate::{Item, ItemInner};

const CHUNK_SIZE: usize = 4096;

use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};
#[derive(Clone)]
pub struct Metrics {
    ids: HashMap<Arc<String>, usize>,
    chunks: Vec<*mut Arc<Item>>,
    register: Sender<ItemInner>,
    len: usize,
}

impl Metrics {
    fn new(register: Sender<ItemInner>) -> Self {
        Self {
            ids: HashMap::new(),
            chunks: Vec::new(),
            register,
            len: 0,
        }
    }
    pub(crate) fn async_register(&self, name: Arc<String>, key: &'static str, t: MetricType) {
        if !self.ids.contains_key(&name) {
            let item = ItemInner::new(name, key, t);
            if let Err(e) = self.register.send(item) {
                log::error!("failed to send metrics. e:{:?}", e);
            }
        }
    }
    #[inline(always)]
    pub(crate) fn get_item(&self, name: &Arc<String>) -> Option<Arc<Item>> {
        self.ids
            .get(name)
            .map(|idx| unsafe { (&*self.get_ptr_mut(*idx)).clone() })
    }
    fn register(&mut self, item: ItemInner) {
        if !self.ids.contains_key(&item.name) {
            // 注册。
            self.reserve();
            let id = self.len;
            self.ids.insert(item.name.clone(), id);
            let item = Arc::new(Item::new(item));
            unsafe { self.get_ptr_mut(id).write(item) };
            self.len += 1;
        }
    }
    fn cap(&self) -> usize {
        self.chunks.len() * CHUNK_SIZE
    }
    unsafe fn get_ptr_mut(&self, idx: usize) -> *mut Arc<Item> {
        debug_assert!(idx <= self.len);
        debug_assert!(idx < self.cap());
        let slot = idx / CHUNK_SIZE;
        let offset = idx % CHUNK_SIZE;
        self.chunks.get_unchecked(slot).offset(offset as isize)
    }
    fn reserve(&mut self) {
        if self.len >= self.cap() {
            use std::mem::MaybeUninit;
            let mut chunk: MaybeUninit<[Arc<Item>; CHUNK_SIZE]> = MaybeUninit::uninit();
            let ptr = chunk.as_mut_ptr() as *mut Arc<Item>;
            self.chunks.push(ptr);
        }
    }
    pub(crate) fn write<W>(&self, w: W) {
        println!("write snapshot");
    }
}
pub struct Metric {
    inited: AtomicBool,
    lock: AtomicBool,
    item: Cell<Arc<Item>>,
    name: Arc<String>,
}
impl Metric {
    #[inline(always)]
    pub(crate) fn from(name: String, key: &'static str, t: MetricType) -> Self {
        let name = Arc::new(name);
        let item = get_metric_or_empty(&name);
        if !item.inited() {
            register_metric(name.clone(), key, t);
        }
        let inited = item.inited();
        Self {
            inited: inited.into(),
            name,
            item: item.into(),
            lock: AtomicBool::new(false),
        }
    }
    #[inline(always)]
    fn try_inited(&self) -> bool {
        // 可能不会及时的感知到初始化成功的状态。用这个减少atomic读取操作。
        if self.inited.load(Ordering::Relaxed) {
            true
        } else {
            std::sync::atomic::fence(Ordering::AcqRel);
            if !self.lock.load(Ordering::Acquire) {
                if let Ok(_) =
                    self.lock
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                {
                    // 初始化成功，则lock状态一直会保持为true. 避免二次初始化
                    match get_metric(&self.name) {
                        Some(item) => {
                            self.item.set(item);
                            self.inited.store(true, Ordering::Release);
                        }
                        None => self.lock.store(false, Ordering::Release),
                    }
                }
            }
            false
        }
    }
}

#[inline]
pub(crate) fn get_metrics<'a>() -> ReadGuard<'a, Metrics> {
    //debug_assert!(METRICS.get().is_some());
    //unsafe { METRICS.get_unchecked().get() }
    todo!();
}

#[inline]
fn register_metric(s: Arc<String>, key: &'static str, ty: MetricType) {
    //debug_assert!(METRICS.get().is_some());
    //get_metrics().async_register(s, key, ty);
}
fn get_metric_or_empty(s: &Arc<String>) -> Arc<Item> {
    get_metric(s).unwrap_or_else(|| Item::empty())
}
#[inline]
fn get_metric(s: &Arc<String>) -> Option<Arc<Item>> {
    //get_metrics().get_item(s)
    None
}

impl Metric {
    #[inline(always)]
    fn item(&self) -> &Item {
        unsafe { &*self.item.as_ptr() }
    }
}
use std::ops::AddAssign;
impl AddAssign<usize> for Metric {
    #[inline(always)]
    fn add_assign(&mut self, rhs: usize) {
        if self.try_inited() {
            self.item().incr(rhs);
        }
    }
}
use std::ops::SubAssign;
impl SubAssign<usize> for Metric {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: usize) {
        if self.try_inited() {
            self.item().decr(rhs);
        }
    }
}

use ds::ReadGuard;
use once_cell::sync::OnceCell;
static METRICS: OnceCell<CowReadHandle<Metrics>> = OnceCell::new();

use ds::{CowReadHandle, CowWriteHandle};
pub fn start_register_metrics() {
    debug_assert!(METRICS.get().is_none());
    let (register_tx, register_rx) = unbounded_channel();
    let (tx, rx) = ds::cow(Metrics::new(register_tx));
    let _ = METRICS.set(rx);
    let registra = MetricRegister::new(register_rx, tx);
    tokio::spawn(registra);
}

unsafe impl Sync for Metrics {}
unsafe impl Send for Metrics {}
unsafe impl Sync for Metric {}
unsafe impl Send for Metric {}
use std::fmt::{self, Debug, Display, Formatter};
impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "metrics =======")
    }
}
impl Display for Metric {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "metric =======")
    }
}

struct MetricRegister {
    rx: Receiver<ItemInner>,
    metrics: CowWriteHandle<Metrics>,
    tick: Interval,
}

impl MetricRegister {
    fn new(rx: Receiver<ItemInner>, metrics: CowWriteHandle<Metrics>) -> Self {
        let mut tick = interval(std::time::Duration::from_secs(17));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self { rx, metrics, tick }
    }
}

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::time::{interval, Interval, MissedTickBehavior};

impl Future for MetricRegister {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        loop {
            ready!(me.tick.poll_tick(cx));
            // 至少有一个，避免不必要的write请求
            if let Some(inner) = ready!(me.rx.poll_recv(cx)) {
                me.metrics.write(|m| {
                    m.register(inner.clone());
                    while let Poll::Ready(Some(item)) = me.rx.poll_recv(cx) {
                        m.register(item);
                    }
                });
            }
        }
    }
}
