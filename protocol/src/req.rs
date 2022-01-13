use crate::{Command, Operation};
use std::fmt::{Debug, Display};
use std::time::Instant;

pub type Context = u64;

pub trait Request: Debug + Display + Send + Sync + 'static + Unpin {
    fn start_at(&self) -> Instant;
    fn operation(&self) -> Operation;
    fn len(&self) -> usize;
    fn hash(&self) -> i64;
    fn on_sent(&mut self);
    fn sentonly(&self) -> bool;
    fn read(&self, oft: usize) -> &[u8];
    fn on_complete(self, resp: Command);
    fn on_err(self, err: crate::Error);
    #[inline(always)]
    fn context_mut(&mut self) -> &mut Context {
        self.mut_context()
    }
    fn mut_context(&mut self) -> &mut Context;
    // 请求成功后，是否需要进行回写或者同步。
    fn write_back(&mut self, wb: bool);
    //fn is_write_back(&self) -> bool;
    // 请求失败后，是否需要进行重试
    fn try_next(&mut self, goon: bool);
}
