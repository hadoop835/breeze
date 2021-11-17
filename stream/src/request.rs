use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;

use crate::callback::RequestCallback;
use protocol::{Command, Error, HashedCommand, Operation};

pub struct Request {
    req: HashedCommand,
    cb: Arc<RequestCallback>,
}

impl protocol::Request for Request {
    #[inline(always)]
    fn len(&self) -> usize {
        self.req.len()
    }
    #[inline(always)]
    fn read(&self, oft: usize) -> &[u8] {
        self.req.read(oft)
    }
    #[inline(always)]
    fn operation(&self) -> Operation {
        self.req.operation()
    }
    #[inline(always)]
    fn hash(&self) -> u64 {
        self.req.hash()
    }
    #[inline(always)]
    fn sentonly(&self) -> bool {
        self.req.sentonly()
    }
    #[inline(always)]
    fn on_sent(&mut self) {
        //log::info!("on sent: req:{} ", self);
    }
    #[inline(always)]
    fn on_complete(self, resp: Command) {
        log::info!(
            "on complete: req:{} response:{}, data:{:?}",
            self,
            resp,
            resp.read(0)
        );
        self.cb.on_complete(self.req, resp);
    }
    #[inline(always)]
    fn on_err(self, err: Error) {
        self.cb.on_err(self.req, err);
    }
}
impl Request {
    #[inline(always)]
    pub fn new(req: HashedCommand, cb: Arc<RequestCallback>) -> Self {
        Self { req, cb }
    }
}

impl Clone for Request {
    fn clone(&self) -> Self {
        panic!("request sould never be cloned!");
    }
}
impl Display for Request {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.req)
    }
}
impl Debug for Request {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.req)
    }
}
impl std::ops::Deref for Request {
    type Target = HashedCommand;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.req
    }
}

impl AsRef<HashedCommand> for Request {
    #[inline(always)]
    fn as_ref(&self) -> &HashedCommand {
        &self.req
    }
}
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

use atomic_waker::AtomicWaker;
pub struct CallbackContext {
    ctx: u32,
    status: AtomicU8,
    request: HashedCommand,
    response: MaybeUninit<Command>,
}
