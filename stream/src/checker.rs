use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, timeout};

use protocol::{Error, Protocol, Request};

use crate::handler::Handler;
use ds::Switcher;
use metrics::MetricId;

pub struct BackendChecker<P, Req> {
    rx: Receiver<Req>,
    run: Switcher,
    finish: Switcher,
    init: Switcher,
    parser: P,
    metric_id: metrics::MetricId,
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
        metric_id: MetricId,
    ) -> Self {
        Self {
            addr: addr.to_string(),
            rx,
            run,
            finish,
            init,
            parser,
            metric_id,
        }
    }
    pub(crate) async fn start_check(&mut self)
    where
        P: Protocol,
        Req: Request,
    {
        let wk = crate::timeout::build_waker();
        while !self.finish.get() {
            let stream = self.try_connect().await;
            let (r, w) = stream.into_split();
            let rx = &mut self.rx;
            let finish = self.finish.clone();
            let wk = wk.clone();
            self.run.on();
            log::info!("handler started:{}", self.metric_id.name());
            if let Err(e) = Handler::from(rx, w, r, self.parser.clone(), finish, wk).await {
                log::info!("{} handler error:{:?}", self.metric_id.name(), e);
            }
            self.run.off();
        }
        log::info!("finished {}. shutting down.", self.metric_id.name());
        self.run.off();
        sleep(Duration::from_secs(15)).await;
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
                    let name = self.metric_id.name();
                    log::warn!("{}-th connecting to {} err:{}", tries, name, e);
                    metrics::status("status", metrics::Status::Down, self.metric_id.id());
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
