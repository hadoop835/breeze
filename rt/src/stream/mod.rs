mod metric;
use ds::ShrinkPolicy;
use metric::MetricStream;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// 1. read/write统计
// 2. 支持write buffer。
// 3. poll_write总是成功
pub struct Stream<S> {
    s: MetricStream<S>,
    idx: usize,
    buf: Vec<u8>,
    write_to_buf: bool,
    cache: metrics::Metric,
    buf_tx: metrics::Metric,
    shrink: ShrinkPolicy,
}
impl<S> From<S> for Stream<S> {
    #[inline]
    fn from(s: S) -> Self {
        let mut me = Self {
            s: s.into(),
            idx: 0,
            buf: Vec::new(),
            write_to_buf: false,
            cache: metrics::Path::base().qps("poll_write_cache"),
            buf_tx: metrics::Path::base().num("mem_buf_tx"),
            shrink: ShrinkPolicy::new(),
        };
        me.grow(1);
        me
    }
}
impl<S: AsyncRead + Unpin + std::fmt::Debug> AsyncRead for Stream<S> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.s).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin + std::fmt::Debug> AsyncWrite for Stream<S> {
    // 先将数据写入到io
    // 未写完的写入到buf
    // 不返回Pending
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        // 这个值应该要大于MSS，否则一个请求分多次返回，会触发delay ack。
        const LARGE_SIZE: usize = 4 * 1024;
        // 数据量比较大，尝试直接写入。写入之前要把buf flush掉。
        if self.buf.len() + data.len() >= LARGE_SIZE {
            let _ = self.as_mut().poll_flush(cx)?;
        }
        let mut oft = 0;
        // 1. buf.len()必须为0；
        // 2. 如果没有显示要求写入到buf, 或者数据量大，则直接写入
        if self.buf.len() == 0 && (!self.write_to_buf || data.len() >= LARGE_SIZE) {
            let _ = Pin::new(&mut self.s).poll_write(cx, data)?.map(|n| oft = n);
        }
        // 未写完的数据写入到buf。
        if oft < data.len() {
            self.grow(data.len() - oft);
            self.cache += 1;
            use ds::Buffer;
            self.buf.write(&data[oft..])
        }
        Poll::Ready(Ok(data.len()))
    }
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if self.buf.len() > 0 {
            let Self { s, idx, buf, .. } = &mut *self;
            let mut w = Pin::new(s);
            while *idx < buf.len() {
                *idx += ready!(w.as_mut().poll_write(cx, &buf[*idx..]))?;
            }
            assert_eq!(*idx, buf.len());
            let flush = w.poll_flush(cx)?;
            assert!(flush.is_ready());
            ready!(flush);
            self.clear();
        }
        Poll::Ready(Ok(()))
    }
    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let _ = self.as_mut().poll_flush(cx);
        Pin::new(&mut self.s).poll_shutdown(cx)
    }
}

impl<S: AsyncWrite + Unpin + std::fmt::Debug> protocol::Writer for Stream<S> {
    #[inline]
    fn pending(&self) -> usize {
        self.buf.len()
    }
    #[inline]
    fn write(&mut self, data: &[u8]) -> protocol::Result<()> {
        if data.len() <= 4 {
            self.buf.write(data)
        } else {
            let noop = noop_waker::noop_waker();
            let mut ctx = Context::from_waker(&noop);
            let _ = Pin::new(self).poll_write(&mut ctx, data);
            Ok(())
        }
    }
    // hint: 提示可能优先写入到cache
    #[inline]
    fn cache(&mut self, hint: bool) {
        if self.write_to_buf != hint {
            self.write_to_buf = hint;
        }
    }
}
impl<S> Stream<S> {
    // send buff扩容策略:
    // 1. 如果buf.len() < buf.capacity(), 则不扩容
    // 2. 新的cap如果小于32k，则按2的指数倍扩容
    // 3. 新的cap如果大于32k，则按4k对齐
    // 4. 最小1k
    #[inline]
    fn grow(&mut self, size: usize) {
        if self.buf.capacity() - self.buf.len() < size {
            let cap = (size + self.buf.len()).max(1024);
            let cap = if cap < 32 * 1024 {
                cap.next_power_of_two()
            } else {
                // 按4k对齐
                (cap + 4095) & !4095
            };
            let grow = cap - self.buf.len();
            self.buf.reserve(grow);
            self.buf_tx += grow;
            assert_eq!(cap, self.buf.capacity());
        }
    }
    // flush的时候尝试缩容。满足以下所有条件
    // 1. buf.len() * 4 <= buf.capacity()
    // 2. 条件1连续满足1024次
    // 3. buf.capacity() >= 4k
    // 每次缩容一半
    #[inline]
    fn clear(&mut self) {
        if self.buf.capacity() >= 2048 && self.buf.len() * 4 <= self.buf.capacity() {
            if self.shrink.tick() {
                let old = self.buf.capacity();
                let new = (self.buf.capacity() / 2).next_power_of_two();
                self.buf.shrink_to(new);
                assert_eq!(new, self.buf.capacity());
                self.buf_tx -= (old - new) as isize;
                log::info!(
                    "tx buf: {},{} => ({}=={}) id:{}",
                    self.buf.len(),
                    old,
                    new,
                    self.buf.capacity(),
                    self.shrink.id()
                );
            }
        } else {
            self.shrink.reset();
        }
        self.idx = 0;
        unsafe { self.buf.set_len(0) };
    }
}
impl<S> Drop for Stream<S> {
    #[inline]
    fn drop(&mut self) {
        self.buf_tx -= self.buf.capacity() as i64;
    }
}
