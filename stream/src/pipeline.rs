use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use ds::{GuardedBuffer, PinnedQueue};
use protocol::{CallbackContext, Endpoint, HashedCommand, Protocol, Result};
use sharding::hash::Hash;

use crate::buffer::{Reader, StreamGuard};
use crate::RequestContext as Request;

pub async fn copy_bidirectional<A, C, P, H>(
    agent: A,
    client: C,
    parser: P,
    _session_id: usize,
    metric_id: usize,
    hasher: H,
) -> Result<()>
where
    A: Endpoint<Item = Request> + Unpin,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
    H: Unpin + Hash,
{
    let buf = GuardedBuffer::new(2048, 1 << 20, 2048, move |_old, delta| {
        metrics::count("mem_buff_rx", delta, metric_id);
    });
    CopyBidirectional {
        rx_buf: buf.into(),
        agent,
        client,
        parser,
        hasher,
        pending: PinnedQueue::new(),
        waker: Default::default(),
        tx_idx: 0,
        tx_buf: Vec::with_capacity(1024),
    }
    .await
}

struct CopyBidirectional<A, C, P, H> {
    rx_buf: StreamGuard,
    agent: A,
    client: C,
    parser: P,
    hasher: H,
    pending: PinnedQueue<CallbackContext>,
    waker: Arc<AtomicWaker>,
    tx_idx: usize,
    tx_buf: Vec<u8>,
}
impl<A, C, P, H> Future for CopyBidirectional<A, C, P, H>
where
    A: Unpin + Endpoint<Item = Request>,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
    H: sharding::hash::Hash + Unpin,
{
    type Output = Result<()>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.waker.register(cx.waker());
        loop {
            // 从client接收数据写入到buffer
            let request = self.poll_fill_buff(cx)?;
            // 解析buffer中的请求，并且发送请求。
            self.parse_request()?;

            // 把已经返回的response，写入到buffer中。
            let response = self.process_pending(cx)?;

            ready!(request);
            ready!(response);
        }
    }
}
impl<A, C, P, H> CopyBidirectional<A, C, P, H>
where
    A: Unpin + Endpoint<Item = Request>,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
    H: sharding::hash::Hash + Unpin,
{
    // 从client读取request流的数据到buffer。
    #[inline(always)]
    fn poll_fill_buff(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let Self { client, rx_buf, .. } = self;
        let mut cx = Context::from_waker(cx.waker());
        let mut rx = Reader::from(client, &mut cx);
        ready!(rx_buf.buf.write(&mut rx))?;
        let num = rx.check_eof_num()?;
        if num == 0 {
            log::info!("buffer full:{}", rx_buf.buf);
        }
        Poll::Ready(Ok(()))
    }
    // 解析buffer，并且发送请求.
    #[inline(always)]
    fn parse_request(&mut self) -> Result<()> {
        use protocol::Stream;
        if self.rx_buf.len() == 0 {
            return Ok(());
        }
        let Self {
            agent,
            parser,
            pending,
            waker,
            hasher,
            rx_buf,
            ..
        } = self;
        // 解析请求，发送请求，并且注册回调
        let mut processor = Visitor {
            agent,
            parser,
            pending,
            waker,
        };
        parser.parse_request(rx_buf, hasher, &mut processor)
    }
    // 处理pending中的请求，并且把数据发送到buffer
    #[inline(always)]
    fn process_pending(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let Self {
            client,
            tx_buf,
            tx_idx,
            pending,
            parser,
            ..
        } = self;
        let mut w = Pin::new(client);
        // 处理回调
        while pending.len() > 0 {
            let cb = unsafe { pending.front_unchecked() };
            if !cb.complete() {
                break;
            }
            let cb = unsafe { pending.pop_front_unchecked() };
            let req = cb.request();
            let resp = cb.response();
            parser.write_response(&req, &resp, tx_buf)?;

            if tx_buf.len() >= 32 * 1024 {
                ready!(Self::poll_flush(cx, tx_idx, tx_buf, w.as_mut()))?;
            }
        }
        Self::poll_flush(cx, tx_idx, tx_buf, w.as_mut())
    }
    // 把response数据flush到client
    #[inline(always)]
    fn poll_flush(
        cx: &mut Context,
        idx: &mut usize,
        buf: &mut Vec<u8>,
        mut writer: Pin<&mut C>,
    ) -> Poll<Result<()>> {
        if buf.len() > 0 {
            while *idx < buf.len() {
                *idx += ready!(writer.as_mut().poll_write(cx, &buf[*idx..]))?;
            }
            *idx = 0;
            unsafe {
                buf.set_len(0);
            }
            ready!(writer.as_mut().poll_flush(cx)?);
        }
        Poll::Ready(Ok(()))
    }
}

struct Visitor<'a, 'b, 'c, 'd, A, P> {
    agent: &'a A,
    parser: &'b P,
    pending: &'c mut PinnedQueue<CallbackContext>,
    waker: &'d AtomicWaker,
}

impl<'a, 'b, 'c, 'd, A, P> protocol::proto::RequestProcessor for Visitor<'a, 'b, 'c, 'd, A, P>
where
    A: Endpoint<Item = Request>,
    P: Protocol,
{
    #[inline(always)]
    fn process(&mut self, cmd: HashedCommand) {
        log::info!("request parsed:{}", cmd);
        let cb = CallbackContext::new(cmd, &self.waker);
        let cb = self.pending.push_back(cb);
        log::info!("request enqueued:{}", cb.request());
        let req = Request::new(cb.as_mut_ptr());
        self.agent.send(req);
    }
}
