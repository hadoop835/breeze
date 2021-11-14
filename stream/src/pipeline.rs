use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use discovery::TopologyTicker;
use ds::GuardedBuffer;
use protocol::{Endpoint, Error, HashedCommand, Protocol, Result};
use sharding::hash::Hash;

use crate::buffer::{Reader, StreamGuard};

use crate::{Request, RequestCallback};

pub async fn copy_bidirectional<A, C, P, H>(
    agent: A,
    client: &mut C,
    parser: P,
    session_id: usize,
    metric_id: usize,
    ticker: TopologyTicker,
    hasher: H,
) -> Result<ConnectStatus>
where
    A: Endpoint<Item = Request> + Unpin,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
    H: Unpin + Hash,
{
    let buf = GuardedBuffer::new(512, 1 * 1024 * 1024, 2 * 1024, move |_old, delta| {
        metrics::count("mem_buff_rx", delta, metric_id);
    });
    CopyBidirectional {
        rx_buf: buf.into(),
        agent,
        client,
        parser,
        hasher,
        pending: VecDeque::with_capacity(31),
        waker: Default::default(),
        tx_idx: 0,
        tx_buf: Vec::with_capacity(1024),
    }
    .await
}

use std::collections::VecDeque;
struct CopyBidirectional<'a, A, C, P, H> {
    rx_buf: StreamGuard,
    agent: A, // 0: master; 1: slave
    client: &'a mut C,
    parser: P,
    hasher: H,
    pending: VecDeque<RequestCallback>,
    waker: Arc<AtomicWaker>,
    tx_idx: usize,
    tx_buf: Vec<u8>,
}
impl<'a, A, C, P, H> Future for CopyBidirectional<'a, A, C, P, H>
where
    A: Unpin + Endpoint<Item = Request>,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
    H: sharding::hash::Hash + Unpin,
{
    type Output = Result<ConnectStatus>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            // 从client接收数据写入到buffer
            let request = self.poll_fill_buff(cx)?;
            // 解析buffer中的请求，并且发送请求。
            self.parse_request()?;

            // 把已经返回的response，写入到buffer中。
            self.process_pending(cx)?;

            // flush buffer
            let flushing = self.poll_flush(cx)?;

            ready!(request);
            ready!(flushing);
        }
    }
}
impl<'a, A, C, P, H> CopyBidirectional<'a, A, C, P, H>
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
        if rx.num() == 0 {
            Poll::Ready(Err(Error::EOF))
        } else {
            Poll::Ready(Ok(()))
        }
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
    fn process_pending(&mut self, cx: &mut Context) -> Result<()> {
        let Self {
            client,
            tx_buf,
            pending,
            parser,
            ..
        } = self;
        let mut cx = Context::from_waker(cx.waker());
        let mut tx = Writer(&mut cx, Pin::new(client), tx_buf);
        // 处理回调
        while let Some(cb) = pending.front() {
            if !cb.complete() {
                break;
            }
            let (req, resp) = cb.take();
            if let Some(resp) = resp {
                //if parser.ok(&resp) {
                parser.write_response(&req, &resp, &mut tx)?;
                //}
                pending.pop_front();
                continue;
            }
            panic!("response parsed error");
        }
        Ok(())
    }
    // 把response数据flush到client
    #[inline(always)]
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        let Self {
            tx_idx,
            tx_buf,
            client,
            ..
        } = self;
        let mut writer = Pin::new(client);
        while *tx_idx < tx_buf.len() {
            *tx_idx += ready!(writer.as_mut().poll_write(cx, &tx_buf[*tx_idx..]))?;
        }
        *tx_idx = 0;
        unsafe {
            tx_buf.set_len(0);
        }
        Poll::Ready(Ok(()))
    }
}

struct Writer<'a, 'c, W>(&'a mut Context<'a>, Pin<&'a mut W>, &'c mut Vec<u8>);

impl<'a, 'c, W> protocol::ResponseWriter for Writer<'a, 'c, W>
where
    W: AsyncWrite,
{
    // 如果
    #[inline(always)]
    fn write(&mut self, data: &[u8]) -> protocol::Result<()> {
        let mut c = 0;
        if data.len() >= 1024 && self.2.len() == 0 {
            if let Poll::Ready(n) = self.1.as_mut().poll_write(self.0, data)? {
                c = n;
            }
        }
        if c < data.len() {
            use ds::vec::Buffer;
            self.2.write(&data[c..]);
        }
        Ok(())
    }
}

struct Visitor<'a, 'b, 'c, 'd, A, P> {
    agent: &'a A,
    parser: &'b P,
    pending: &'c mut VecDeque<RequestCallback>,
    waker: &'d Arc<AtomicWaker>,
}

impl<'a, 'b, 'c, 'd, A, P> protocol::proto::RequestProcessor for Visitor<'a, 'b, 'c, 'd, A, P>
where
    A: Endpoint<Item = Request>,
    P: Protocol,
{
    #[inline(always)]
    fn process(&mut self, cmd: HashedCommand) {
        log::info!("request parsed:{}", cmd);
        let op = self.parser.operation(&cmd) as usize;
        debug_assert!(op <= 1);
        let cb = RequestCallback::new(self.waker.clone());
        if !cmd.sentonly() {
            self.pending.push_back(cb.clone());
        }
        let req = Request::new(cmd, cb);
        self.agent.send(req);
    }
}

pub enum ConnectStatus {
    EOF,   // 关闭agent与client的物理连接
    Reuse, // 只关闭agent的逻辑连接，复用与client的物理连接
}
