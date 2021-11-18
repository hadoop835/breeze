use std::fmt::{self, Debug, Display, Formatter};

use crate::callback::CallbackContext;
use protocol::{Command, Error, HashedCommand, Operation};

pub struct Request {
    ctx: *mut CallbackContext,
}

impl protocol::Request for Request {
    #[inline(always)]
    fn len(&self) -> usize {
        self.req().len()
    }
    #[inline(always)]
    fn read(&self, oft: usize) -> &[u8] {
        self.req().read(oft)
    }
    #[inline(always)]
    fn operation(&self) -> Operation {
        self.req().operation()
    }
    #[inline(always)]
    fn hash(&self) -> u64 {
        self.req().hash()
    }
    #[inline(always)]
    fn sentonly(&self) -> bool {
        self.req().sentonly()
    }
    #[inline(always)]
    fn on_sent(&mut self) {
        self.ctx().on_sent();
    }
    #[inline(always)]
    fn on_complete(self, resp: Command) {
        log::info!(
            "on complete: req:{} response:{}, data:{:?}",
            self,
            resp,
            resp.read(0)
        );
        self.ctx().on_complete(resp);
    }
    #[inline(always)]
    fn on_err(self, err: Error) {
        self.ctx().on_err(err);
    }
}
impl Request {
    #[inline(always)]
    pub fn new(ctx: *mut CallbackContext) -> Self {
        Self { ctx }
    }

    #[inline(always)]
    fn req(&self) -> &HashedCommand {
        unsafe { &(&*self.ctx).request() }
    }
    #[inline(always)]
    fn ctx(&self) -> &mut CallbackContext {
        unsafe { &mut *self.ctx }
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
        write!(f, "{}", self.req())
    }
}
impl Debug for Request {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.req())
    }
}
//impl std::ops::Deref for Request {
//    type Target = HashedCommand;
//    #[inline(always)]
//    fn deref(&self) -> &Self::Target {
//        &self.req
//    }
//}
//
//impl AsRef<HashedCommand> for Request {
//    #[inline(always)]
//    fn as_ref(&self) -> &HashedCommand {
//        &self.req
//    }
//}

unsafe impl Send for Request {}
unsafe impl Sync for Request {}
