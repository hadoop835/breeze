use crate::ResizedRingBuffer;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};
use std::sync::Arc;

use super::RingSlice;

pub trait BuffRead {
    type Out;
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out);
}

pub struct GuardedBuffer {
    inner: ResizedRingBuffer,
    taken: usize, // 已取走未释放的位置 read <= taken <= write
    guards: VecDeque<(u32, Arc<CopyOnRecall>)>,
}

impl GuardedBuffer {
    pub fn new<F: Fn(usize, isize) + 'static>(min: usize, max: usize, init: usize, cb: F) -> Self {
        Self {
            inner: ResizedRingBuffer::from(min, max, init, cb),
            guards: VecDeque::with_capacity(32 - 1),
            taken: 0,
        }
    }
    #[inline]
    pub fn write<R, O>(&mut self, r: &mut R) -> O
    where
        R: BuffRead<Out = O>,
    {
        while let Some((len, guard)) = self.guards.front_mut() {
            //if !guard.recall(|| self.inner.data().sub_slice(0, *len as usize).data()) {
            if guard.status() as u8 == Status::Released as u8 {
                self.inner.advance_read(*len as usize);
                self.guards.pop_front();
                continue;
            }
            break;
            // 成功recall. 可以释放当前资源
        }
        let b = self.inner.as_mut_bytes();
        let (n, out) = r.read(b);
        self.inner.advance_write(n);
        out
    }
    #[inline]
    pub fn read(&self) -> RingSlice {
        self.inner
            .slice(self.taken, self.inner.writtened() - self.taken)
    }
    #[inline(always)]
    pub fn take(&mut self, n: usize) -> MemGuard {
        log::info!("mem taken:{} buffer:{}", n, self);
        debug_assert!(n > 0);
        debug_assert!(self.taken + n <= self.writtened());
        let data = self.inner.slice(self.taken, n);
        let guard: MemGuard = data.into();
        self.taken += n;
        self.guards.push_back((n as u32, guard.guard.clone()));
        guard
    }
    #[inline]
    fn pending(&self) -> usize {
        self.taken - self.inner.read()
    }
    #[inline(always)]
    pub fn update(&mut self, idx: usize, val: u8) {
        let oft = self.offset(idx);
        self.inner.update(oft, val);
    }
    #[inline(always)]
    pub fn at(&self, idx: usize) -> u8 {
        self.inner.at(self.offset(idx))
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len() - self.pending()
    }
    #[inline(always)]
    fn offset(&self, oft: usize) -> usize {
        self.pending() + oft
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for GuardedBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "taken:{} {}", self.taken, self.inner)
    }
}

pub struct MemGuard {
    share: RingSlice,
    guard: Arc<CopyOnRecall>,
}

#[repr(u8)]
#[derive(Copy, Clone)]
enum Status {
    Reading = 0u8,
    Recalled = 1,
    Released = 2,
}
const STATUSES: [Status; 3] = [Reading, Recalled, Released];

impl MemGuard {
    // f: 会被调用1~2次。
    #[inline]
    pub fn read(&self, oft: usize) -> &[u8] {
        match self.guard.status() {
            // 1: 说明当前状态是正常读取中
            Reading => self.share.read(oft),
            // 说明内存已经被recall
            Released => self.guard.data(),
            Recalled => {
                // 内存被recall。更新状态，后续读取会使用copy内存。share内存可以被安全recall
                // 这里面不直接通知，而是由recaller自己调度获取状态变化
                self.guard.release();
                self.guard.data()
            }
        }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        // 即使内存被recall了，长度不会被改变
        // 可以安全使用
        self.share.len()
    }
    // 把共享内存拷贝到guard中，后续共享内存可以直接回收。
    #[inline(always)]
    pub fn recall(&self) {
        self.guard.recall(|| self.share.data());
    }
}
impl From<RingSlice> for MemGuard {
    #[inline(always)]
    fn from(data: RingSlice) -> Self {
        Self {
            share: data,
            guard: Arc::new(CopyOnRecall::new()),
        }
    }
}

impl Drop for MemGuard {
    #[inline]
    fn drop(&mut self) {
        log::info!("memguard dropped:{:?}", self.read(0));
        self.guard.release();
    }
}

pub struct CopyOnRecall {
    status: AtomicU8, // share内存是可读，不可读即可以释放或者已经失败
    copy: AtomicPtr<Vec<u8>>,
}

impl CopyOnRecall {
    #[inline(always)]
    fn new() -> Self {
        Self {
            status: AtomicU8::new(Reading as u8),
            copy: AtomicPtr::default(),
        }
    }
    // ring buffer内存不足时，会触发回收。把ringslice的资源回收。
    // 回收之前，如果数据还处理可用状态，触发copy-on-recall把数据写入到cos中
    #[inline(always)]
    fn recall<C: Fn() -> Vec<u8>>(&self, cp: C) -> bool {
        match self.status() {
            Released => true,
            Reading => {
                let mut data = cp();
                let empty = self.copy.swap(&mut data, Ordering::AcqRel);
                debug_assert!(empty.is_null());
                self.status.store(Recalled as u8, Ordering::Release);
                false
            }
            Recalled => false,
        }
    }
    #[inline(always)]
    fn release(&self) {
        self.status.store(Released as u8, Ordering::Release);
    }
    #[inline(always)]
    fn ptr(&self) -> *mut Vec<u8> {
        self.copy.load(Ordering::Acquire)
    }
    #[inline(always)]
    fn data(&self) -> &[u8] {
        log::info!("data:{}, ptr:{}", self.status() as u8, self.ptr() as usize);
        debug_assert!(self.status.load(Ordering::Acquire) != Reading as u8);
        debug_assert!(!self.copy.load(Ordering::Acquire).is_null());
        unsafe { &*self.copy.load(Ordering::Acquire) }
    }
    #[inline(always)]
    fn status(&self) -> Status {
        self.status.load(Ordering::Acquire).into()
    }
}

use Status::*;
impl From<u8> for Status {
    #[inline(always)]
    fn from(s: u8) -> Status {
        STATUSES[s as usize]
    }
}
use std::ops::{Deref, DerefMut};

impl Deref for GuardedBuffer {
    type Target = ResizedRingBuffer;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for GuardedBuffer {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
