use std::collections::VecDeque;

// queue在扩缩容的时候，原有的数据不移动。
pub struct PinnedQueue<T> {
    data: VecDeque<T>,
}

impl<T> PinnedQueue<T> {
    #[inline(always)]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            data: VecDeque::with_capacity(cap),
        }
    }
    // 把数据推入back，并且返回原有的引用
    #[inline(always)]
    pub fn push_back(&mut self, t: T) -> &mut T {
        let old = self.data.capacity();
        self.data.push_back(t);
        if old != self.data.capacity() {
            log::error!("capacity scaled from {} to {}", old, self.data.capacity());
        }
        if let Some(back) = self.data.back_mut() {
            return back;
        }
        panic!("never run here");
    }
    #[inline(always)]
    pub unsafe fn front_unchecked(&self) -> &T {
        if let Some(t) = self.data.front() {
            return t;
        }
        panic!("queue empty");
    }
    #[inline(always)]
    pub unsafe fn take_front_unchecked(&mut self) -> T {
        debug_assert!(self.len() > 0);
        if let Some(t) = self.data.pop_front() {
            return t;
        }
        panic!("take empty queue");
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.data.len()
    }
}
