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
    guards: VecDeque<(u32, Arc<CopyOnRecall>)>,
}

impl GuardedBuffer {
    pub fn new<F: Fn(usize, isize) + 'static>(min: usize, max: usize, init: usize, cb: F) -> Self {
        Self {
            inner: ResizedRingBuffer::from(min, max, init, cb),
            guards: VecDeque::with_capacity(32 - 1),
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
    #[inline(always)]
    pub fn take(&mut self, n: usize) -> MemGuard {
        let data = self.inner.data().sub_slice(0, n);
        let status = Arc::new(CopyOnRecall::new());
        self.guards.push_back((n as u32, status.clone()));
        MemGuard {
            share: data,
            guard: status,
        }
    }
    //#[inline(always)]
    //pub fn slice(&mut self) -> RingSlice {
    //    self.inner.data()
    //}
    //#[inline(always)]
    //pub fn update(&mut self, idx: usize, val: u8) {
    //    self.inner.update(idx, val);
    //}
    //#[inline(always)]
    //pub fn len(&self) -> usize {
    //    self.inner.len()
    //}
    //#[inline(always)]
    //pub fn at(&self, idx: usize) -> u8 {
    //    self.inner.at(idx)
    //}
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

pub struct CopyOnRecall {
    status: AtomicU8,                // share内存是可读，不可读即可以释放或者已经失败
    copy: AtomicPtr<(usize, usize)>, // 将ResizedBuffer将share回收时，如果avail为true，则会将内存拷贝一份。
}

impl Drop for CopyOnRecall {
    #[inline(always)]
    fn drop(&mut self) {
        let slice = *self.copy.get_mut();
        if !slice.is_null() {
            unsafe {
                let (ptr, cap) = *slice;
                debug_assert!(ptr > 0);
                debug_assert!(cap > 0);
                let _ = Vec::from_raw_parts(ptr as *mut u8, cap, cap);
            }
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
                let mut copy = std::mem::ManuallyDrop::new(cp());
                self.copy.store(
                    &mut (copy.as_mut_ptr() as usize, copy.capacity()),
                    Ordering::Release,
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
        debug_assert_eq!(self.status.load(Ordering::Acquire), Recalling as u8);
        debug_assert!(!self.copy.load(Ordering::Acquire).is_null());
        unsafe {
            let (ptr, cap) = *self.copy.load(Ordering::Acquire);
            std::slice::from_raw_parts(ptr as *const u8, cap)
        }
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
