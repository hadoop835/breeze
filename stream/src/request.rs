use std::fmt::{self, Debug, Display, Formatter};

use crate::callback::RequestCallback;
use protocol::{Command, Error, HashedCommand, Operation};

pub struct Request {
    req: HashedCommand,
    cb: RequestCallback,
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
        log::info!("on sent: req:{} ", self);
    }
    #[inline(always)]
    fn on_complete(self, resp: Command) {
        log::info!("complete: req:{} response:{}", self, resp);
        self.cb.on_complete(self.req, resp);
    }
    #[inline(always)]
    fn on_err(self, err: Error) {
        self.cb.on_err(self.req, err);
    }
}
impl Request {
    #[inline(always)]
    pub fn new(req: HashedCommand, cb: RequestCallback) -> Self {
        log::info!("on new: req:{} ", req);
        Self { req, cb }
    }
}

//impl Clone for Request {
//    fn clone(&self) -> Self {
//        panic!("request sould never be cloned!");
//    }
//}
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
