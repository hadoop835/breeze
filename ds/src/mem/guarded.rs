use crate::ResizedRingBuffer;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};

use super::RingSlice;

pub trait BuffRead {
    type Out;
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out);
}

pub struct GuardedBuffer {
    inner: ResizedRingBuffer,
    taken: usize, // 已取走未释放的位置 read <= taken <= write
    guards: VecDeque<AtomicU32>,
}

impl GuardedBuffer {
    pub fn new<F: Fn(usize, isize) + 'static>(min: usize, max: usize, init: usize, cb: F) -> Self {
        Self {
            inner: ResizedRingBuffer::from(min, max, init, cb),
            guards: VecDeque::with_capacity(2047),
            taken: 0,
        }
    }
    #[inline]
    pub fn write<R, O>(&mut self, r: &mut R) -> O
    where
        R: BuffRead<Out = O>,
    {
        while let Some(guard) = self.guards.front_mut() {
            let guard = guard.load(Ordering::Acquire);
            if guard == 0 {
                break;
            }
            self.inner.advance_read(guard as usize);
            self.guards.pop_front();
        }
        let b = self.inner.as_mut_bytes();
        let (n, out) = r.read(b);
        self.inner.advance_write(n);
        out
    }
    #[inline(always)]
    pub fn read(&self) -> RingSlice {
        self.inner
            .slice(self.taken, self.inner.writtened() - self.taken)
    }
    #[inline(always)]
    pub fn take(&mut self, n: usize) -> MemGuard {
        debug_assert!(n > 0);
        debug_assert!(self.taken + n <= self.writtened());
        self.guards.push_back(AtomicU32::new(0));
        let data = self.inner.slice(self.taken, n);
        self.taken += n;
        if let Some(guard) = self.guards.back() {
            let ptr = guard as *const AtomicU32;
            return MemGuard::new(data, ptr);
        }
        panic!("never run here");
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
    mem: RingSlice,
    guard: *const AtomicU32,
}

impl MemGuard {
    #[inline(always)]
    fn new(data: RingSlice, guard: *const AtomicU32) -> Self {
        debug_assert!(!guard.is_null());
        unsafe { debug_assert_eq!((&*guard).load(Ordering::Acquire), 0) };
        Self {
            mem: data,
            guard: guard,
        }
    }
}
impl Drop for MemGuard {
    #[inline]
    fn drop(&mut self) {
        debug_assert!(!self.guard.is_null());
        unsafe {
            debug_assert_eq!((&*self.guard).load(Ordering::Acquire), 0);
            log::info!("mem guard released:{}", self.mem.len());
            (&*self.guard).store(self.mem.len() as u32, Ordering::Release);
        }
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
impl Deref for MemGuard {
    type Target = RingSlice;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}

impl DerefMut for MemGuard {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mem
    }
}
