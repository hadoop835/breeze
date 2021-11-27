use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use protocol::{Protocol, Request, Result};
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};
use tokio::sync::mpsc::Receiver;

use crate::buffer::StreamGuard;
use crate::timeout::TimeoutWaker;
use ds::{GuardedBuffer, PinnedQueue, Switcher};

pub(crate) struct Handler<'r, Req, P, W, R> {
    data: &'r mut Receiver<Req>,
    pending: PinnedQueue<Req>,

    tx: BufWriter<W>,
    rx: R,
    finish: Switcher,

    // 用来处理发送数据
    cache: Option<Req>, // pending的尾部数据是否需要发送。true: 需要发送
    oft_c: usize,
    // 处理接收数据
    buf: StreamGuard,
    parser: P,
    flushing: bool,
}
impl<'r, Req, P, W, R> Future for Handler<'r, Req, P, W, R>
where
    Req: Request + Unpin,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<()>;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        while !me.finish.get() {
            let request = me.poll_request(cx)?;
            let _flush = me.poll_flush(cx)?;
            let response = me.poll_response(cx)?;
            if me.pending.len() > 0 {
                ready!(response);
            }
            ready!(request);
        }
        Poll::Ready(Ok(()))
    }
}
impl<'r, Req, P, W, R> Handler<'r, Req, P, W, R> {
    pub(crate) fn from(
        data: &'r mut Receiver<Req>,
        tx: W,
        rx: R,
        parser: P,
        finish: Switcher,
        _wk: TimeoutWaker,
    ) -> Self
    where
        W: AsyncWrite + Unpin,
    {
        Self {
            data,
            cache: None,
            pending: PinnedQueue::new(),
            tx: BufWriter::with_capacity(2048, tx),
            rx,
            parser,
            finish,
            oft_c: 0,
            buf: GuardedBuffer::new(2048, 1 << 20, 2048, |_, _| {}).into(),
            flushing: false,
        }
    }
    // 发送request
    #[inline(always)]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        Req: Request,
        W: AsyncWrite + Unpin,
    {
        loop {
            if let Some(ref req) = self.cache {
                log::info!("data sent: {}", req);
                while self.oft_c < req.len() {
                    let data = req.read(self.oft_c);
                    self.oft_c += ready!(Pin::new(&mut self.tx).poll_write(cx, data))?;
                }
                let mut req = self.cache.take().expect("take cache");
                req.on_sent();
                if !req.sentonly() {
                    self.pending.push_back(req);
                }
                self.oft_c = 0;
                self.flushing = true;
            }
            self.cache = ready!(self.data.poll_recv(cx));
            // 当前的设计下，data不会返回None.channel不会被close掉
            debug_assert!(self.cache.is_some());
        }
    }
    #[inline(always)]
    fn poll_response(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        R: AsyncRead + Unpin,
        P: Protocol,
        Req: Request,
    {
        loop {
            let mut cx = Context::from_waker(cx.waker());
            let mut reader = crate::buffer::Reader::from(&mut self.rx, &mut cx);
            ready!(self.buf.buf.write(&mut reader))?;
            // num == 0 说明是buffer满了。等待下一次事件，buffer释放后再读取。
            let num = reader.check_eof_num()?;
            log::info!("{} bytes received.", num);
            if num == 0 {
                return Poll::Ready(Ok(()));
            }
            use protocol::Stream;
            while self.buf.len() > 0 {
                match self.parser.parse_response(&mut self.buf)? {
                    None => break,
                    Some(cmd) => {
                        let req = unsafe { self.pending.pop_front_unchecked() };
                        req.on_complete(cmd);
                    }
                }
            }
        }
    }
    #[inline(always)]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        W: AsyncWrite + Unpin,
    {
        ready!(Pin::new(&mut self.tx).poll_flush(cx))?;
        self.flushing = false;
        Poll::Ready(Ok(()))
    }
}
unsafe impl<'r, Req, P, W, R> Send for Handler<'r, Req, P, W, R> {}
unsafe impl<'r, Req, P, W, R> Sync for Handler<'r, Req, P, W, R> {}
