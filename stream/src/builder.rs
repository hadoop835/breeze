use std::sync::{
    atomic::AtomicBool,
    atomic::Ordering::{Acquire, Release},
    Arc,
};

use ds::chan::mpsc::{channel, Sender, TrySendError};

use ds::Switcher;

use crate::checker::BackendChecker;
use endpoint::{Builder, Endpoint, Single, Timeout};
use metrics::Path;
use protocol::{Error, Protocol, Request, ResOption, Resource};

#[derive(Clone)]
pub struct BackendBuilder<P, R> {
    _marker: std::marker::PhantomData<(P, R)>,
}

impl<P: Protocol, R: Request> Builder<P, R, Backend<R>> for BackendBuilder<P, R> {
    fn auth_option_build(
        //todo 这个传string会减少一次copy
        addr: &str,
        parser: P,
        rsrc: Resource,
        service: &str,
        timeout: Timeout,
        option: ResOption,
    ) -> Backend<R> {
        Backend::from((addr, parser, rsrc, service, timeout, option))
    }
}

impl<R: Request, P: Protocol> From<(&str, P, Resource, &str, Timeout, ResOption)> for Backend<R> {
    fn from(
        (addr, parser, rsrc, service, timeout, option): (
            &str,
            P,
            Resource,
            &str,
            Timeout,
            ResOption,
        ),
    ) -> Self {
        let (tx, rx) = channel(256);
        let finish: Switcher = false.into();
        let init: Switcher = false.into();
        let f = finish.clone();
        let path = Path::new(vec![rsrc.name(), service]);
        let single = Arc::new(AtomicBool::new(false));
        let checker =
            BackendChecker::from(addr, rx, f, init.clone(), parser, path, timeout, option);
        let s = single.clone();
        rt::spawn(checker.start_check(s));

        Backend {
            inner: BackendInner {
                finish,
                init,
                tx,
                single,
            }
            .into(),
        }
    }
}

#[derive(Clone)]
pub struct Backend<R> {
    inner: Arc<BackendInner<R>>,
}

pub struct BackendInner<R> {
    single: Arc<AtomicBool>,
    tx: Sender<R>,
    // 实例销毁时，设置该值，通知checker，会议上check.
    finish: Switcher,
    // 由checker设置，标识是否初始化完成。
    init: Switcher,
}

impl<R> discovery::Inited for Backend<R> {
    // 已经连接上或者至少连接了一次
    #[inline]
    fn inited(&self) -> bool {
        self.inner.init.get()
    }
}

impl<R> Drop for BackendInner<R> {
    fn drop(&mut self) {
        self.finish.on();
    }
}

impl<R: Request> Endpoint for Backend<R> {
    type Item = R;
    #[inline]
    fn send(&self, req: R) {
        if let Err(e) = self.inner.tx.try_send(req) {
            match e {
                TrySendError::Closed(r) => r.on_err(Error::ChanWriteClosed),
                TrySendError::Full(r) => r.on_err(Error::ChanFull),
                TrySendError::Disabled(r) => r.on_err(Error::ChanDisabled),
            }
        }
    }

    #[inline]
    fn available(&self) -> bool {
        self.inner.tx.get_enable()
    }
}
impl<R> Single for Backend<R> {
    fn single(&self) -> bool {
        self.inner.single.load(Acquire)
    }
    fn enable_single(&self) {
        self.inner.single.store(true, Release);
    }
    fn disable_single(&self) {
        self.inner.single.store(false, Release);
    }
}
