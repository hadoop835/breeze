use tokio::sync::mpsc::{error::TrySendError, Sender};

use ds::Switcher;
use protocol::{Endpoint, Error, Request};

unsafe impl<Req> Send for MpmcStream<Req> {}
unsafe impl<Req> Sync for MpmcStream<Req> {}

// 支持并发读取的stream
pub struct MpmcStream<Req> {
    tx: Sender<Req>,
    run: Switcher,
    // 持有的远端资源地址（ip:port）
    metric_id: usize,
}

impl<Req> MpmcStream<Req> {
    // id必须小于parallel
    #[inline]
    pub fn new(tx: Sender<Req>, metric_id: usize, run: Switcher) -> Self {
        Self { tx, metric_id, run }
    }
}

impl<Req> Endpoint for MpmcStream<Req>
where
    Req: Request,
{
    type Item = Req;
    #[inline(always)]
    fn send(&self, req: Self::Item) {
        use metrics::MetricName;
        log::debug!("{} send request:{}", self.metric_id.name(), req);
        if self.run.get() {
            if let Err(e) = self.tx.try_send(req) {
                match e {
                    TrySendError::Closed(r) => r.on_err(Error::QueueFull),
                    TrySendError::Full(r) => r.on_err(Error::QueueFull),
                }
            }
        } else {
            req.on_err(Error::Closed);
        }
    }
}
