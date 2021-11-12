use std::sync::Arc;

use tokio::sync::mpsc::channel;

use crate::checker::BackendChecker;
use crate::{MpmcStream, Request};
use ds::Switcher;
use protocol::{Endpoint, Protocol, Resource};

#[derive(Clone)]
pub struct BackendBuilder<P> {
    _marker: std::marker::PhantomData<P>,
}

impl<P: Protocol> protocol::Builder<P, Backend> for BackendBuilder<P> {
    fn build(addr: &str, parser: P, rsrc: Resource, service: &str) -> Backend {
        let (tx, rx) = channel(32);
        let finish: Switcher = false.into();
        let init: Switcher = false.into();
        let run: Switcher = false.into();
        let mid = metrics::register!(rsrc.name(), service, addr);
        let stream = Arc::new(MpmcStream::new(tx, mid.id(), run.clone()));
        let f = finish.clone();
        let mut checker = BackendChecker::from(addr, rx, run, f, init.clone(), parser, mid);
        tokio::spawn(async move { checker.start_check().await });
        Backend {
            stream,
            finish,
            init,
        }
    }
}

#[derive(Clone)]
pub struct Backend {
    finish: Switcher,
    init: Switcher,
    stream: Arc<MpmcStream<Request>>,
}

impl discovery::Inited for Backend {
    // 已经连接上或者至少连接了一次
    #[inline]
    fn inited(&self) -> bool {
        self.init.get()
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.finish.off();
    }
}

impl Endpoint for Backend {
    type Item = Request;
    #[inline(always)]
    fn send(&self, req: Request) {
        self.stream.send(req);
    }
}
