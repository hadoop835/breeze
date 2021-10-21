use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering::*};

pub struct LockGuard<'a, T: 'a> {
    lock: &'a AtomicBool,
    data: &'a mut T,
}

use std::ops::{Deref, DerefMut};
impl<'a, T> Deref for LockGuard<'a, T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &*self.data
    }
}
impl<'a, T> DerefMut for LockGuard<'a, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.data
    }
}

#[derive(Default)]
pub struct Lock<T> {
    lock: AtomicBool,
    data: UnsafeCell<T>,
}

impl<T> Lock<T> {
    #[inline]
    pub fn new(t: T) -> Self {
        Self {
            lock: AtomicBool::new(false),
            data: UnsafeCell::new(t),
        }
    }
    #[inline(always)]
    pub fn lock(&self) -> LockGuard<T> {
        self.obtain_lock();
        LockGuard {
            lock: &self.lock,
            data: unsafe { &mut *self.data.get() },
        }
    }
    #[inline(always)]
    pub fn try_lock(&self) -> Option<LockGuard<T>> {
        if self.try_obtain_lock() {
            Some(LockGuard {
                lock: &self.lock,
                data: unsafe { &mut *self.data.get() },
            })
        } else {
            None
        }
    }
    #[inline(always)]
    fn obtain_lock(&self) {
        while !self.try_obtain_lock() {
            std::hint::spin_loop();
        }
    }
    #[inline(always)]
    fn try_obtain_lock(&self) -> bool {
        if let Ok(_) = self.lock.compare_exchange(false, true, AcqRel, Relaxed) {
            true
        } else {
            false
        }
    }
}
impl<'a, T> Drop for LockGuard<'a, T> {
    #[inline(always)]
    fn drop(&mut self) {
        self.lock.store(false, Release);
    }
}
