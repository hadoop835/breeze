use std::borrow::Borrow;
use std::pin::Pin;
use std::task::{Context, Poll};
use protocol::{MetaType, Operation, Protocol};
use crate::{AsyncReadAll, AsyncWriteAll, MpmcStream, Request, Response};
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use ds::Cid;

pub struct MetaBackend<P> {
    id: Cid,
    inner: Arc<MpmcStream>,
    parser: P,
    request: Option<Request>
}

impl<P> MetaBackend<P>
where
    P: Protocol + Unpin,
{
    pub fn from(id: Cid, stream: Arc<MpmcStream>, parser: P) -> Self {
        Self {
            id,
            inner: stream,
            parser,
            request: None
        }
    }
}

impl<P> AsyncReadAll for MetaBackend<P>
    where
        P: Protocol + Unpin,
{
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        if me.request.is_some() {
            let response = me.parser.generate_meta_response(&me.request.as_ref().unwrap());
            if response.is_some() {
                let id = me.request.as_ref().unwrap().id();
                me.request.take();
                return Poll::Ready(Ok(Response::from(id, response.unwrap(), me.id.id(), me.inner.clone())))
            }
        }
        me.request.take();
        Poll::Ready(Err(Error::new(ErrorKind::InvalidInput, "unknown meta commands")))
    }
}

impl<P> AsyncWriteAll for MetaBackend<P>
    where
        P: Protocol + Unpin,
{
    #[inline(always)]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        me.request = Some(buf.clone());
        Poll::Ready(Ok(()))
    }
}
