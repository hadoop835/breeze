use std::collections::VecDeque;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, timeout};

use protocol::{Error, Protocol, Request};

use crate::handler::Handler;
use ds::{GuardedBuffer, Switcher};
use metrics::{Metric, Path};

pub struct BackendChecker<P, Req> {
    rx: Receiver<Req>,
    run: Switcher,
    finish: Switcher,
    init: Switcher,
    parser: P,
    s_metric: Metric,
    addr: String,
}

impl<P, Req> BackendChecker<P, Req> {
    pub(crate) fn from(
        addr: &str,
        rx: Receiver<Req>,
        run: Switcher,
        finish: Switcher,
        init: Switcher,
        parser: P,
        path: &Path,
    ) -> Self {
        Self {
            addr: addr.to_string(),
            rx,
            run,
            finish,
            init,
            parser,
            s_metric: path.status("status"),
        }
    }
    pub(crate) async fn start_check(&mut self)
    where
        P: Protocol,
        Req: Request,
    {
        while !self.finish.get() {
            let stream = self.try_connect().await;
            let (r, w) = stream.into_split();
            let rx = &mut self.rx;
            self.run.on();
            log::info!("handler started:{}", self.s_metric);
            use crate::buffer::StreamGuard;
            use crate::gc::DelayedDrop;
            let mut buf: DelayedDrop<_> =
                StreamGuard::from(GuardedBuffer::new(2048, 1 << 20, 16 * 1024, |_, _| {})).into();
            let mut pending = VecDeque::with_capacity(31);
            if let Err(e) =
                Handler::from(rx, &mut pending, &mut buf, w, r, self.parser.clone()).await
            {
                log::info!("{} handler error:{:?}", self.s_metric, e);
            }
            // 先关闭，关闭之后不会有新的请求发送
            self.run.off();
            // 1. 把未处理的请求提取出来，通知。
            // 在队列中未发送的。
            let noop = noop_waker::noop_waker();
            let mut ctx = std::task::Context::from_waker(&noop);
            use std::task::Poll;
            // 有请求在队列中未发送。
            while let Poll::Ready(Some(req)) = rx.poll_recv(&mut ctx) {
                req.on_err(Error::Pending);
            }
            // 2. 有请求已经发送，但response未获取到
            while let Some(req) = pending.pop_front() {
                req.on_err(Error::Waiting);
            }
            // buf需要延迟释放
            crate::gc::delayed_drop(buf);
        }
        debug_assert!(!self.run.get());
        log::info!("{} closed", self.s_metric);
    }
    async fn try_connect(&mut self) -> TcpStream
    where
        P: Protocol,
        Req: Request,
    {
        let mut tries = 0u64;
        loop {
            log::debug!("try to connect {} tries:{}", self.addr, tries);
            match self.reconnected_once().await {
                Ok(stream) => {
                    self.init.on();
                    return stream;
                }
                Err(e) => {
                    self.init.on();
                    log::warn!("{}-th connecting to {} err:{}", tries, self.s_metric, e);
                    self.s_metric += 1;
                }
            }

            tries += 1;
            sleep(Duration::from_secs((1 << tries).min(8))).await;
        }
    }
    #[inline]
    async fn reconnected_once(&self) -> std::result::Result<TcpStream, Box<dyn std::error::Error>>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
        Req: Request + Send + Sync + Unpin + 'static,
    {
        let stream = timeout(Duration::from_secs(2), TcpStream::connect(&self.addr)).await??;
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }
}
