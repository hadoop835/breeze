use std::sync::atomic::{AtomicUsize, Ordering::*};

use super::CacheAligned;

// 基于数组实现的无索map。key是递增的数。value <= bounded
pub struct SeqMap {
    // 正向索引。
    indice: Vec<CacheAligned<AtomicUsize>>,
    reverse: Vec<CacheAligned<AtomicUsize>>,
}

// Value有最大值的限制。
impl SeqMap {
    #[inline]
    pub fn bounded(cap: usize) -> Self {
        let cap = cap.next_power_of_two();
        let indice = (0..cap).map(|_| Default::default()).collect();
        let reverse = (0..cap).map(|_| Default::default()).collect();
        Self { indice, reverse }
    }
    #[inline(always)]
    pub fn insert(&self, key: usize, val: usize) {
        debug_assert!(val < self.indice.len());
        unsafe {
            self.indice
                .get_unchecked(self.index(key))
                .0
                .store(val, Release);
            self.reverse.get_unchecked(val).0.store(key, Release);
        }
    }
    // 如果key对应的值不存在，则会panic。
    #[inline(always)]
    pub fn load(&self, key: usize) -> usize {
        unsafe {
            let val = self.indice.get_unchecked(self.index(key)).0.load(Acquire);
            if key == self.reverse.get_unchecked(val).0.load(Acquire) {
                val
            } else {
                for (v, k) in self.reverse.iter().enumerate() {
                    if k.0.load(Acquire) == key {
                        return v;
                    }
                }
                panic!("no value found for key:{}", key);
            }
        }
    }
    #[inline(always)]
    fn index(&self, key: usize) -> usize {
        (self.indice.len() - 1) & key
    }
}
