use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use ds::BuffRead;

pub(crate) struct Reader<'a, 'c, C> {
    n: usize, // 成功读取的数据
    client: &'a mut C,
    cx: &'c mut Context<'c>,
}

impl<'a, 'c, C> Reader<'a, 'c, C> {
    #[inline(always)]
    pub(crate) fn from(client: &'a mut C, cx: &'c mut Context<'c>) -> Self {
        let n = 0;
        Self { n, client, cx }
    }
    // 上一次请求读取到的字节数
    #[inline(always)]
    pub(crate) fn num(&self) -> usize {
        self.n
    }
}

impl<'a, 'c, C> BuffRead for Reader<'a, 'c, C>
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
    processed: usize,
    pub(crate) buf: GuardedBuffer,
}
impl protocol::Stream for StreamGuard {
    #[inline(always)]
    fn update(&mut self, idx: usize, val: u8) {
        let oft = self.offset(idx);
        self.buf.update(oft, val);
    }
    #[inline(always)]
    fn at(&self, idx: usize) -> u8 {
        self.buf.at(self.offset(idx))
    }
    #[inline(always)]
    fn take(&mut self, n: usize) -> MemGuard {
        self.processed += n;
        self.buf.take(n)
    }
    #[inline(always)]
    fn len(&self) -> usize {
        self.buf.len() - self.pending()
    }
    #[inline(always)]
    fn slice(&self) -> RingSlice {
        let len = self.len();
        let oft = self.pending();
        self.buf.data().sub_slice(oft, len - oft)
    }
}
impl StreamGuard {
    #[inline(always)]
    fn offset(&self, oft: usize) -> usize {
        self.pending() + oft
    }
    #[inline(always)]
    fn pending(&self) -> usize {
        self.processed - self.buf.read()
    }
    pub fn new() -> Self {
        Self {
            processed: 0,
            buf: GuardedBuffer::new(2048, 1024 * 1024, 2048, |_, _| {}),
        }
    }
}

impl From<GuardedBuffer> for StreamGuard {
    #[inline]
    fn from(buf: GuardedBuffer) -> Self {
        Self {
            processed: 0,
            buf: buf,
        }
    }
}
