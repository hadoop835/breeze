use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use ds::GuardedBuffer;
use protocol::{HashedCommand, Protocol, Result, Topology};

use crate::buffer::{Reader, StreamGuard};
use crate::{Callback, CallbackContext, CallbackContextPtr, Request, RequestIdGen};

pub async fn copy_bidirectional<A, C, P>(
    agent: A,
    client: C,
    parser: P,
    session_id: usize,
    metric_id: usize,
    cb: Callback,
) -> Result<()>
where
    A: Unpin + Topology<Item = Request>,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    let buf = GuardedBuffer::new(2048, 1 << 20, 2048, move |_old, delta| {
        metrics::count("mem_buff_rx", delta, metric_id);
    });
    CopyBidirectional {
        rx_buf: buf.into(),
        agent,
        client,
        parser,
        pending: VecDeque::with_capacity(127),
        waker: Default::default(),
        tx_idx: 0,
        tx_buf: Vec::with_capacity(1024),
        id_gen: RequestIdGen::new(metric_id, session_id),
        cb,
    }
    .await
}

// TODO TODO CopyBidirectional在退出时，需要确保不存在pending中的请求，否则需要会存在内存访问异常。
struct CopyBidirectional<A, C, P> {
    rx_buf: StreamGuard,
    agent: A,
    client: C,
    parser: P,
    pending: VecDeque<CallbackContextPtr>,
    waker: Arc<AtomicWaker>,
    tx_idx: usize,
    tx_buf: Vec<u8>,
    id_gen: RequestIdGen,
    cb: Callback,
}
impl<A, C, P> Future for CopyBidirectional<A, C, P>
where
    A: Unpin + Topology<Item = Request>,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
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
impl<A, C, P> CopyBidirectional<A, C, P>
where
    A: Unpin + Topology<Item = Request>,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    // 从client读取request流的数据到buffer。
    #[inline(always)]
    fn poll_fill_buff(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let Self { client, rx_buf, .. } = self;
        let mut cx = Context::from_waker(cx.waker());
        let mut rx = Reader::from(client, &mut cx);
        loop {
            ready!(rx_buf.buf.write(&mut rx))?;
            let num = rx.check_eof_num()?;
            // buffer full
            if num == 0 {
                break;
            }
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
            rx_buf,
            id_gen,
            cb,
            ..
        } = self;
        // 解析请求，发送请求，并且注册回调
        let mut processor = Visitor {
            agent,
            pending,
            waker,
            id_gen,
            cb,
        };
        parser.parse_request(rx_buf, agent.hasher(), &mut processor)
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
        while let Some(cb) = pending.front_mut() {
            if !cb.complete() {
                break;
            }
            let req = cb.request();
            if cb.inited() {
                let resp = unsafe { cb.response() };
                parser.write_response(req, resp, tx_buf)?;

                // 请求成功，并且需要进行write back
                if cb.is_write_back() && resp.is_ok() {
                    use protocol::Operation;
                    if req.flag().get_operation() != Operation::Store {
                        let exp = 86400;
                        let new = parser.convert_to_writeback_request(req, resp, exp);
                        cb.with_request(new);
                    }
                    cb.async_start_write_back();
                }
            } else {
                parser.write_response_on_err(req, tx_buf)?;
            }

            // 无论是否开户write back，都可以安全的从pending中剔除。
            pending.pop_front();

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

struct Visitor<'a, 'b, 'c, 'd, 'e, A> {
    agent: &'a A,
    pending: &'c mut VecDeque<CallbackContextPtr>,
    waker: &'d AtomicWaker,
    id_gen: &'b mut RequestIdGen,
    cb: &'e Callback,
}

impl<'a, 'b, 'c, 'd, 'e, A> protocol::proto::RequestProcessor for Visitor<'a, 'b, 'c, 'd, 'e, A>
where
    A: Topology<Item = Request>,
{
    #[inline(always)]
    fn process(&mut self, cmd: HashedCommand) {
        // 特别关注： 在CopyBidirectional持有了agent，在CopyBidirectional异常退出时，如果有
        // pending中的请求（pending.len>0），可能导致agent是一个dangle pointer。出现UB.
        // 需要在CopyBidirectional退出时，确保pending.len()为0
        let id = self.id_gen.next();
        let mut ctx: CallbackContextPtr =
            CallbackContext::new(id, cmd, &self.waker, self.cb.clone()).into();
        let req: Request = ctx.build_request();
        self.pending.push_back(ctx);
        self.agent.send(req);
    }
}
