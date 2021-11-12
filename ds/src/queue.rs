pub fn mpsc<T>() -> (Sender<T>, Receiver<T>) {
    todo!();
}

pub struct Sender<T> {
    tx: T,
}
impl<T> Sender<T> {
    pub fn send(&self, t: T) {
        todo!();
    }
    pub fn try_send(&self, t: T) -> Result<(), T> {
        todo!();
    }
}
pub struct Receiver<T> {
    tx: T,
}
use std::task::{Context, Poll};

impl<T> Receiver<T> {
    pub fn poll_recv(&mut self, cx: &mut Context) -> Poll<Option<T>> {
        Poll::Pending
    }
}
