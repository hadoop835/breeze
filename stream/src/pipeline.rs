use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use discovery::TopologyTicker;
use ds::GuardedBuffer;
use protocol::{Endpoint, HashedCommand, Protocol, Result};

use crate::buffer::{Reader, StreamGuard};

use crate::{Request, RequestCallback, Writer};

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
    H: Unpin + sharding::hash::Hash,
{
    let buf = GuardedBuffer::new(512, 1 * 1024 * 1024, 2 * 1024, move |_old, delta| {
        metrics::count("mem_buff_rx", delta, metric_id);
    });
    CopyBidirectional {
        buf: buf.into(),
        agent,
        client,
        parser,
        hasher,
        pending: VecDeque::with_capacity(31),
        waker: Default::default(),
    }
    .await
}

use std::collections::VecDeque;
struct CopyBidirectional<'c, A, C, P, H> {
    buf: StreamGuard,
    agent: A, // 0: master; 1: slave
    client: &'c mut C,
    parser: P,
    hasher: H,
    pending: VecDeque<RequestCallback>,
    waker: Arc<AtomicWaker>,
}
impl<'c, A, C, P, H> Future for CopyBidirectional<'c, A, C, P, H>
where
    A: Unpin + Endpoint<Item = Request>,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
    H: sharding::hash::Hash + Unpin,
{
    type Output = Result<ConnectStatus>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let CopyBidirectional {
            buf,
            agent,
            client,
            parser,
            hasher,
            pending,
            waker,
        } = &mut *self;
        let mut cx = Context::from_waker(cx.waker());
        let mut client = Reader::from(client, &mut cx);
        loop {
            // 从client接收数据写入到buffer
            let request = buf.buf.write(&mut client)?;
            log::info!("{} bytes read from client", client.num());
            // 解析请求，发送请求，并且注册回调
            let mut processor = Visitor {
                agent,
                parser,
                pending,
                waker,
            };
            parser.parse_request(buf, hasher, &mut processor)?;

            // 处理回调
            while let Some(cb) = pending.front() {
                if !cb.complete() {
                    break;
                }
                let (req, resp) = cb.take();
                if let Some(resp) = resp {
                    if parser.ok(&resp) {
                        //parser.write_response(req, resp, self);
                    }
                }
                pending.pop_front();
            }
            if client.num() == 0 {
                break;
            }
        }

        Poll::Ready(Ok(ConnectStatus::EOF))
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
