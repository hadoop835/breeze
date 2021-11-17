use std::collections::VecDeque;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::buffer::StreamGuard;
use crate::timeout::TimeoutWaker;
//use ds::seq_map::Receiver;
use ds::{GuardedBuffer, Switcher};
use futures::ready;
use protocol::{Protocol, Request, Result};
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};
use tokio::sync::mpsc::Receiver;

pub(crate) struct Handler<'r, Req, P, W, R> {
    data: &'r mut Receiver<Req>,
    // 已接收，未收到response的请求
    cache: *mut ManuallyDrop<Req>,
    pending: VecDeque<Req>,

    tx: BufWriter<W>,
    rx: R,
    finish: Switcher,
    waker: TimeoutWaker,

    // 用来处理发送数据
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
            let _ = me.poll_flush(cx)?;
            let response = me.poll_response(cx)?;

            if me.pending.len() > 0 {
                me.waker.register(cx.waker());
            } else {
                me.waker.unregister();
            }
            ready!(request);
            ready!(response);
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
        waker: TimeoutWaker,
    ) -> Self
    where
        W: AsyncWrite + Unpin,
    {
        Self {
            data,
            cache: 0 as *mut ManuallyDrop<Req>,
            pending: VecDeque::with_capacity(16 - 1),
            tx: BufWriter::with_capacity(2048, tx),
            rx,
            parser,
            finish,
            waker,
            oft_c: 0,
            buf: GuardedBuffer::new(2048, 1024 * 1024, 2048, |_, _| {}).into(),
            flushing: false,
        }
    }
    #[inline(always)]
    fn poll_request(&mut self, cx: &mut Context) -> Poll<Result<()>>
    where
        Req: Request,
        W: AsyncWrite + Unpin,
    {
        loop {
            if self.cache.is_null() {
                let req = ready!(self.data.poll_recv(cx)).expect("req chan closed");
                self.set(req);
            }
            let len = self.cache().len();
            while self.oft_c < len {
                let r: &Req = unsafe { std::mem::transmute(self.cache as *mut Req) };
                let data = r.read(self.oft_c);
                log::info!("request data:{:?}", data);
                self.oft_c += ready!(Pin::new(&mut self.tx).poll_write(cx, data))?;
                self.flushing = true;
            }
            self.oft_c = 0;
            let mut req = self.take();
            req.on_sent();
            log::info!("request sent:{}", req);
            if !req.sentonly() {
                self.pending.push_back(req);
            }
        }
    }
    #[inline(always)]
    fn cache(&self) -> &mut Req {
        unsafe { std::mem::transmute(self.cache as *mut Req) }
    }
    #[inline(always)]
    fn set(&mut self, req: Req) {
        debug_assert!(self.cache.is_null());
        let mut req = ManuallyDrop::new(req);
        self.cache = &mut req as *mut _;
    }
    #[inline(always)]
    fn take(&mut self) -> Req {
        debug_assert!(!self.cache.is_null());
        let req: Req = unsafe { ManuallyDrop::take(&mut *self.cache) };
        self.cache = 0 as *mut _;
        req
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
            log::info!("{} response received", reader.num());
            if reader.num() == 0 {
                break; // EOF or Buffer full
            }
            loop {
                match self.parser.parse_response(&mut self.buf) {
                    None => break,
                    Some(cmd) => {
                        let req = self.pending.pop_front().expect("no pending req");
                        req.on_complete(cmd);
                    }
                }
            }
        }
        Poll::Ready(Err(protocol::Error::EOF))
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
