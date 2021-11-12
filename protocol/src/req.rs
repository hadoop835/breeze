use crate::{Command, Operation};
use std::fmt::{Debug, Display};
pub trait Request: Debug + Display + Send + Sync + 'static + Unpin {
    fn operation(&self) -> Operation;
    fn len(&self) -> usize;
    fn hash(&self) -> u64;
    fn on_sent(&mut self);
    fn sentonly(&self) -> bool;
    fn read(&self, oft: usize) -> &[u8];
    fn on_complete(self, resp: Command);
    fn on_err(self, err: crate::Error);
}
