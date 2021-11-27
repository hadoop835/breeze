use crate::{Command, Operation};
use std::fmt::{Debug, Display};

pub type Context = u64;

pub trait Request: Debug + Display + Send + Sync + 'static + Unpin {
    fn operation(&self) -> Operation;
    fn len(&self) -> usize;
    fn hash(&self) -> u64;
    fn on_sent(&mut self);
    fn sentonly(&self) -> bool;
    fn read(&self, oft: usize) -> &[u8];
    fn on_complete(self, resp: Command);
    fn on_err(self, err: crate::Error);
    fn mut_context(&mut self) -> &mut Context;
    fn write_back(&mut self, wb: bool);
    fn is_write_back(&self) -> bool;
    fn goon(&mut self, goon: bool);
}
