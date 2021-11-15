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
            if !guard.recall(|| self.inner.data().sub_slice(0, *len as usize).data()) {
                break;
            }
            // 成功recall. 可以释放当前资源
            self.inner.advance_read(*len as usize);
            self.guards.pop_front();
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
        println!("mem taken:{} buffer:{}", n, self.inner);
        let data = self.inner.slice(self.taken, n);
        self.taken += n;
        let status = Arc::new(CopyOnRecall::new());
        self.guards.push_back((n as u32, status.clone()));
        MemGuard {
            share: data,
            guard: status,
        }
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
        write!(f, "{}", self.inner)
    }
}

pub struct MemGuard {
    share: RingSlice,
    guard: Arc<CopyOnRecall>,
}

#[repr(u8)]
#[derive(Copy, Clone)]
enum Status {
    GcSave = 0u8,
    Reading = 1,
    Recalling = 2,
}

impl MemGuard {
    #[inline]
    pub fn read(&self, oft: usize) -> &[u8] {
        debug_assert!(oft < self.share.len());
        match self.guard.status.load(Ordering::Acquire).into() {
            // 1: 说明当前状态是正常读取中
            Reading => self.share.read(oft),
            // 说明内存已经被recall
            GcSave => self.guard.data(),
            Recalling => {
                // 内存被recall。更新状态，后续读取会使用copy内存。share内存可以被安全recall
                // 这里面不直接通知，而是由recaller自己调度获取状态变化
                self.guard.clear();
                self.guard.data()
            }
        }
    }
    #[inline]
    pub fn len(&self) -> usize {
        // 即使内存被recall了，长度不会被改变
        // 可以安全使用
        self.share.len()
    }
}

impl Drop for MemGuard {
    #[inline]
    fn drop(&mut self) {
        self.guard.clear();
    }
}

use std::mem::ManuallyDrop;
pub struct CopyOnRecall {
    status: AtomicU8, // share内存是可读，不可读即可以释放或者已经失败
    copy: AtomicPtr<ManuallyDrop<Vec<u8>>>, // 将ResizedBuffer将share回收时，如果avail为true，则会将内存拷贝一份。
}

impl Drop for CopyOnRecall {
    #[inline(always)]
    fn drop(&mut self) {
        let slice = *self.copy.get_mut();
        if !slice.is_null() {
            println!("dropping:{}", slice as usize);
            unsafe {
                let v = &mut *slice;
                println!("dropped data:{:?}", v.len());
                ManuallyDrop::drop(v);
            }
            println!("dropped");
        }
    }
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
        match self.status.load(Ordering::Acquire).into() {
            GcSave => true,
            Reading => {
                let mut copy = ManuallyDrop::new(cp());
                println!("recalling. data:{:?} {}", copy, copy.as_ptr() as usize);
                let empty = self.copy.swap(&mut copy, Ordering::AcqRel);
                debug_assert!(empty.is_null());
                self.status.store(Recalling as u8, Ordering::Release);
                let data = self.data();
                println!(
                    "recalling data-2:{:?} -- {}",
                    data,
                    self.copy.load(Ordering::Acquire) as usize
                );
                false
            }
            Recalling => false,
        }
    }
    #[inline(always)]
    fn clear(&self) {
        self.status.store(GcSave as u8, Ordering::Release);
    }
    #[inline(always)]
    fn data(&self) -> &[u8] {
        debug_assert!(self.status.load(Ordering::Acquire) != Reading as u8);
        debug_assert!(!self.copy.load(Ordering::Acquire).is_null());
        let data = self.copy.load(Ordering::Acquire);
        unsafe {
            let v: &mut ManuallyDrop<Vec<u8>> = &mut *data;
            println!("len:{}", v.len());
            debug_assert!(v.len() < 1024 * 1024);
        }
        unsafe { &*self.copy.load(Ordering::Acquire) }
    }
}

const STATUSES: [Status; 3] = [GcSave, Reading, Recalling];
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
