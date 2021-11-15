use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use ds::BuffRead;

pub(crate) struct Reader<'a, C> {
    n: usize, // 成功读取的数据
    client: &'a mut C,
    cx: &'a mut Context<'a>,
}

impl<'a, C> Reader<'a, C> {
    #[inline(always)]
    pub(crate) fn from(client: &'a mut C, cx: &'a mut Context<'a>) -> Self {
        let n = 0;
        Self { n, client, cx }
    }
    // 上一次请求读取到的字节数
    #[inline(always)]
    pub(crate) fn num(&self) -> usize {
        self.n
    }
}

impl<'a, C> BuffRead for Reader<'a, C>
where
    C: AsyncRead + Unpin,
{
    type Out = Poll<Result<()>>;
    #[inline(always)]
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out) {
        let mut rb = ReadBuf::new(b);
        let Self { n, client, cx } = self;
        let out = Pin::new(&mut **client).poll_read(cx, &mut rb);
        *n = rb.capacity() - rb.remaining();
        (*n, out)
    }
}
use ds::{GuardedBuffer, MemGuard, RingSlice};
// 已写入未处理的数据流。
pub struct StreamGuard {
    pub(crate) buf: GuardedBuffer,
}
impl protocol::Stream for StreamGuard {
    #[inline(always)]
    fn update(&mut self, idx: usize, val: u8) {
        self.buf.update(idx, val);
    }
    #[inline(always)]
    fn at(&self, idx: usize) -> u8 {
        self.buf.at(idx)
    }
    #[inline(always)]
    fn take(&mut self, n: usize) -> MemGuard {
        self.buf.take(n)
    }
    #[inline(always)]
    fn len(&self) -> usize {
        self.buf.len()
    }
    #[inline(always)]
    fn slice(&self) -> RingSlice {
        self.buf.read()
    }
}
impl From<GuardedBuffer> for StreamGuard {
    #[inline]
    fn from(buf: GuardedBuffer) -> Self {
        Self { buf: buf }
    }
}
