use std::sync::Arc;

use tokio::sync::mpsc::channel;

use crate::checker::BackendChecker;
use crate::MpmcStream;
use ds::Switcher;
use protocol::{Endpoint, Protocol, Request, Resource};

#[derive(Clone)]
pub struct BackendBuilder<P, R> {
    _marker: std::marker::PhantomData<(P, R)>,
}

impl<P: Protocol, R: Request> protocol::Builder<P, R, Backend<R>> for BackendBuilder<P, R> {
    fn build(addr: &str, parser: P, rsrc: Resource, service: &str) -> Backend<R> {
        let (tx, rx) = channel(256);
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
pub struct Backend<R> {
    finish: Switcher,
    init: Switcher,
    stream: Arc<MpmcStream<R>>,
}

impl<R> discovery::Inited for Backend<R> {
    // 已经连接上或者至少连接了一次
    #[inline]
    fn inited(&self) -> bool {
        self.init.get()
    }
}

impl<R> Drop for Backend<R> {
    fn drop(&mut self) {
        self.finish.off();
    }
}

impl<R: Request> Endpoint for Backend<R> {
    type Item = R;
    #[inline(always)]
    fn send(&self, req: R) {
        self.stream.send(req);
    }
}
