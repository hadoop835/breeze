use crate::CallbackContext;
use protocol::{Command, Context, Error, HashedCommand, Operation};
use std::fmt::{self, Debug, Display, Formatter};
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
        self.req().flag().get_operation()
    }
    #[inline(always)]
    fn hash(&self) -> u64 {
        self.req().hash()
    }
    #[inline(always)]
    fn sentonly(&self) -> bool {
        self.req().is_sentonly()
    }
    #[inline(always)]
    fn on_sent(&mut self) {
        self.ctx().on_sent();
    }
    #[inline(always)]
    fn on_complete(self, resp: Command) {
        self.ctx().on_complete(resp);
    }
    #[inline(always)]
    fn on_err(self, err: Error) {
        self.ctx().on_err(err);
    }
    #[inline(always)]
    fn mut_context(&mut self) -> &mut Context {
        self.ctx().ctx.as_mut_flag()
    }
    #[inline(always)]
    fn write_back(&mut self, wb: bool) {
        self.ctx().ctx.write_back(wb);
    }
    #[inline(always)]
    fn is_write_back(&self) -> bool {
        self.ctx().ctx.is_write_back()
    }
    #[inline(always)]
    fn goon(&mut self, goon: bool) {
        self.ctx().ctx.goon(goon);
    }
}
impl Request {
    #[inline(always)]
    pub fn new(ctx: *mut CallbackContext) -> Self {
        Self { ctx }
    }

    #[inline(always)]
    fn req(&self) -> &HashedCommand {
        self.ctx().request()
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
        write!(f, "{}", self.ctx())
    }
}
impl Debug for Request {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

unsafe impl Send for Request {}
unsafe impl Sync for Request {}

pub struct RequestId {
    metric_id: usize,
    session_id: usize,
    seq: usize,
}

impl RequestId {
    #[inline(always)]
    pub fn new(metric_id: usize, session_id: usize, seq: usize) -> Self {
        Self {
            metric_id,
            session_id,
            seq,
        }
    }
    #[inline(always)]
    fn id(&self) -> u128 {
        (self.session_id as u128) | (self.seq as u128)
    }
}
impl Display for RequestId {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "metric:{} id:{}", self.metric_id, self.id(),)
    }
}
impl Debug for RequestId {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use metrics::MetricName;
        write!(f, "metric:{} id:{}", self.metric_id.name(), self.id())
    }
}

pub struct RequestIdGen {
    metric_id: usize,
    session_id: usize,
    seq: usize,
}

impl RequestIdGen {
    #[inline(always)]
    pub fn new(metric_id: usize, session_id: usize) -> Self {
        Self {
            metric_id,
            session_id,
            seq: 0,
        }
    }
    #[inline(always)]
    pub fn next(&mut self) -> RequestId {
        let id = RequestId::new(self.metric_id, self.session_id, self.seq);
        self.seq += 1;
        id
    }
    #[inline(always)]
    pub fn seq(&self) -> usize {
        self.seq
    }
}
