use std::io::{Error, ErrorKind, Result};

use discovery::Inited;
use protocol::Protocol;

//use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};

pub use protocol::endpoint::Endpoint;

macro_rules! define_topology {
    ($($top:ty, $item:ident, $ep:expr);+) => {

 #[derive(Clone)]
 pub enum Topology<B, E, R, P> {
      $($item($top)),+
 }

 impl<B, E, R, P> Topology<B, E, R, P>  {
     pub fn try_from(parser:P, endpoint:&str) -> Result<Self> {
          match &endpoint[..]{
              $($ep => Ok(Self::$item(parser.into())),)+
              _ => Err(Error::new(ErrorKind::InvalidData, format!("'{}' is not a valid endpoint", endpoint))),
          }
     }
 }
 impl<B, E, R, P> Inited for Topology<B, E, R, P> where E:Inited {
     #[inline]
     fn inited(&self) -> bool {
          match self {
              $(
                  Self::$item(p) => p.inited(),
              )+
          }
     }
 }

impl<B, E, R, P> discovery::TopologyWrite for Topology<B, E, R, P> where P:Sync+Send+Protocol, B:protocol::Builder<P, R, E>, E:Endpoint<Item = R>{
    #[inline]
    fn update(&mut self, name: &str, cfg: &str) {
        match self {
             $(Self::$item(s) => discovery::TopologyWrite::update(s, name, cfg),)+
        }
    }
}

impl<B, E, R, P> protocol::topo::Topology<E,R> for Topology<B, E, R, P> where P:Sync+Send+Protocol, E:Endpoint<Item = R>, R:protocol::Request{
    #[inline(always)]
    fn hasher(&self) -> &str {
        match self {
            $(
                Self::$item(p) => p.hasher(),
            )+
        }
    }
}

impl<B, E, R, P> protocol::Endpoint for Topology<B, E, R, P>
where P:Sync+Send+Protocol, E:Endpoint<Item = R>,
    R: protocol::Request,
    P: Protocol,
{
    type Item = R;
    #[inline(always)]
    fn send(&self, req:Self::Item) {
        match self {
            $(
                Self::$item(p) => p.send(req),
            )+
        }
    }

}


    };
}

use crate::cacheservice::topo::CacheService;
//use pipe::{Pipe, PipeTopology};

define_topology! {
//PipeTopology, Pipe,         Pipe,         "pipe";
    CacheService<B, E, R, P>, CacheService, "cs"
}
