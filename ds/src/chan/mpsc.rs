use std::task::{Context, Poll};

use crate::Switcher;

pub enum TrySendError<T> {
    Closed(T),
    Full(T),
    Disabled(T),
}

pub fn channel<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let s: Switcher = false.into();
    let (tx, rx) = tokio::sync::mpsc::channel(cap);
    let tx = Sender {
        switcher: s.clone(),
        inner: tx,
    };
    let rx = Receiver {
        switcher: s,
        inner: rx,
    };
    (tx, rx)
}

pub struct Receiver<T> {
    switcher: Switcher,
    inner: tokio::sync::mpsc::Receiver<T>,
}
pub struct Sender<T> {
    switcher: Switcher,
    inner: tokio::sync::mpsc::Sender<T>,
}

impl<T> Receiver<T> {
    #[inline]
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner.poll_recv(cx)
    }
    pub fn enable(&mut self) {
        self.switcher.on();
    }
    pub fn disable(&mut self) {
        self.switcher.off();
    }
    #[inline]
    pub fn running(&self) -> bool {
        self.switcher.get()
    }
}

impl<T> Sender<T> {
    #[inline]
    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        if self.switcher.get() {
            self.inner.try_send(message).map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(t) => TrySendError::Full(t),
                tokio::sync::mpsc::error::TrySendError::Closed(t) => TrySendError::Closed(t),
            })
        } else {
            Err(TrySendError::Disabled(message))
        }
    }
}
