use super::RingBuffer;
use std::time::{Duration, Instant};

type Callback = Box<dyn Fn(usize, isize)>;
// 支持自动扩缩容的ring buffer。
// 扩容时机：在reserve_bytes_mut时触发扩容判断。如果当前容量满，或者超过4ms时间处理过的内存未通过reset释放。
// 缩容时机： 每写入回收内存时判断，如果连续1分钟使用率小于25%，则缩容一半
pub struct ResizedRingBuffer {
    // 在resize之后，不能立即释放ringbuffer，因为有可能还有外部引用。
    // 需要在所有的processed的字节都被ack之后（通过reset_read）才能释放
    max_processed: usize,
    old: Vec<RingBuffer>,
    inner: RingBuffer,
    // 下面的用来做扩缩容判断
    min: u32,
    max: u32,
    scale_up_tick_num: u32,
    scale_up_tick: Instant,
    scale_in_tick_num: u32,
    scale_in_tick: Instant,
    on_change: Option<Callback>,
}

use std::ops::{Deref, DerefMut};

impl Deref for ResizedRingBuffer {
    type Target = RingBuffer;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ResizedRingBuffer {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl ResizedRingBuffer {
    // 最小512个字节，最大4M. 初始化为4k.
    pub fn new() -> Self {
        Self::from(512, 4 * 1024 * 1024, 4 * 1024)
    }
    pub fn from(min: usize, max: usize, init: usize) -> Self {
        assert!(min <= init && init <= max);
        Self {
            max_processed: std::usize::MAX,
            old: Vec::new(),
            inner: RingBuffer::with_capacity(init),
            min: min as u32,
            max: max as u32,
            scale_up_tick_num: 0,
            scale_up_tick: Instant::now(),
            scale_in_tick_num: 0,
            scale_in_tick: Instant::now(),
            on_change: None,
        }
    }
    // 需要写入数据时，判断是否需要扩容
    #[inline(always)]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        if self.inner.available() {
            if self.scale_up_tick_num > 0 {
                self.scale_up_tick_num = 0;
            }
            self.inner.as_mut_bytes()
        } else {
            if self.scale_up_tick_num == 0 {
                self.scale_up_tick = Instant::now();
            }
            self.scale_up_tick_num += 1;
            const D: Duration = Duration::from_millis(4);
            // 所有的已读数据都已处理完成，立即扩容
            // 连续超过4ms需要扩容
            let scale = self.read() == self.processed() || self.scale_up_tick.elapsed() >= D;
            if scale && self.cap() * 2 <= self.max as usize {
                self._resize(self.cap() * 2);
            }
            self.inner.as_mut_bytes()
        }
    }
    // 有数写入时，判断是否需要缩容
    #[inline]
    pub fn advance_write(&mut self, n: usize) {
        self.inner.advance_write(n);
        // 判断是否需要缩容
        if self.cap() > self.min as usize {
            // 当前使用的buffer小于1/4.
            if self.len() * 4 <= self.cap() {
                if self.scale_in_tick_num == 0 {
                    self.scale_in_tick = Instant::now();
                }
                self.scale_in_tick_num += 1;
                if self.scale_in_tick_num & 1023 == 0 {
                    const D: Duration = Duration::from_secs(60);
                    if self.scale_in_tick.elapsed() >= D {
                        let new = self.cap() / 2;
                        self._resize(new);
                    }
                }
            } else {
                self.scale_in_tick_num = 0;
            }
        }
    }
    #[inline]
    fn _resize(&mut self, cap: usize) {
        assert!(cap <= self.max as usize);
        assert!(cap >= self.min as usize);
        let new = self.inner.resize(cap);
        let old = std::mem::replace(&mut self.inner, new);
        self.max_processed = old.processed();
        if let Some(ref f) = self.on_change {
            f(old.cap(), cap as isize);
        }
        self.old.push(old);
    }
    #[inline(always)]
    pub fn reset_read(&mut self, read: usize) {
        self.inner.reset_read(read);
        if read >= self.max_processed {
            let delta = self.old.iter().fold(0usize, |mut s, b| {
                s += b.cap();
                s
            });
            if let Some(ref f) = self.on_change {
                f(self.cap(), delta as isize * -1);
            }
            self.old.clear();
            self.max_processed = std::usize::MAX;
        }
    }
    #[inline]
    pub fn set_on_resize<F: Fn(usize, isize) + 'static>(&mut self, f: F) {
        self.on_change = Some(Box::new(f));
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for ResizedRingBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "rrb:(inner:{}, old:{:?})", self.inner, self.old)
    }
}
