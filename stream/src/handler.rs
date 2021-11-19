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
    pending: VecDeque<Req>,

    tx: BufWriter<W>,
    rx: R,
    finish: Switcher,
    waker: TimeoutWaker,

    // 用来处理发送数据
    cache: bool, // pending的尾部数据是否需要发送。true: 需要发送
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

            //if me.pending.len() > 0 {
            //    me.waker.register(cx.waker());
            //} else {
            //    me.waker.unregister();
            //}
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
        waker: TimeoutWaker,
    ) -> Self
    where
        W: AsyncWrite + Unpin,
    {
        Self {
            data,
            cache: false,
            pending: VecDeque::with_capacity(2047),
            tx: BufWriter::with_capacity(2048, tx),
            rx,
            parser,
            finish,
            waker,
            oft_c: 0,
            buf: GuardedBuffer::new(1 << 20, 1 << 20, 1 << 20, |_, _| {}).into(),
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
            if !self.cache {
                let req = ready!(self.data.poll_recv(cx)).expect("req chan closed");
                log::info!("request polled :{} ", req,);
                self.pending.push_back(req);
                self.cache = true;
                debug_assert_eq!(self.oft_c, 0);
            }
            debug_assert_eq!(self.cache, true);
            debug_assert!(self.pending.len() > 0);
            if let Some(req) = self.pending.back_mut() {
                while self.oft_c < req.len() {
                    let data = req.read(self.oft_c);
                    self.oft_c += ready!(Pin::new(&mut self.tx).poll_write(cx, data))?;
                }
                self.flushing = true;
                req.on_sent();
                if req.sentonly() {
                    self.pending.pop_back();
                }
            }
            self.cache = false;
            self.oft_c = 0;
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
            let num = reader.check_eof_num()?;
            log::info!("{} response received {}", num, self.buf.buf);
            if num == 0 {
                log::info!("buffer full:{}", self.buf.buf);
                // 等待下一次调整
                return Poll::Ready(Ok(()));
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
