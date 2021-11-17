mod seq;

use futures::Stream;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

use cacheservice::CacheService;
use discovery::{Inited, TopologyRead};
use protocol::Protocol;
use topology::Topology as ServiceTopology;
// <<<<<<< HEAD
use redisservice::RedisService;
use stream::{AsyncReadAll, AsyncWriteAll, BackendStream, LayerRole, Request, Response};
// =======

// use stream::{AsyncReadAll, AsyncWriteAll, BackendStream, LayerRole, Request, Response};
// >>>>>>> redis_conn_manage

#[derive(Clone)]
pub enum Topology<P> {
    RedisService(ServiceTopology<P>),
    CacheService(ServiceTopology<P>),
}

// <<<<<<< HEAD
impl<P> Topology<P> {
    pub fn try_from(parser: P, endpoint: String) -> Result<Self> {
        match &endpoint[..] {
            "rs" => Ok(Self::RedisService(parser.into())),
            "cs" => Ok(Self::CacheService(parser.into())),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("'{}' is not a valid endpoint", endpoint),
            )),
        }
    }
}

impl<P> Inited for Topology<P> {
    fn inited(&self) -> bool {
        match self {
            Self::RedisService(r) => r.inited(),
            Self::CacheService(c) => c.inited(),
        }
    }
}
// =======
//        impl<P> Topology<P>  {
//            pub fn try_from(parser:P, endpoint:String) -> Result<Self> {
//                 match &endpoint[..]{
//                     $($ep => Ok(Self::$item(parser.into())),)+
//                     _ => Err(Error::new(ErrorKind::InvalidData, format!("'{}' is not a valid endpoint", endpoint))),
//                 }
//            }

//             pub fn to_concrete_topo(&self) -> Box<&dyn ServiceTopo> {
//                 match self {
//                     $(
//                         Self::$item(t) => Box::new(t),
//                     )+
//                 }
//             }

//        }
//        impl<P> Inited for Topology<P> {
//            fn inited(&self) -> bool {
//                 match self {
//                     $(
//                         Self::$item(p) => p.inited(),
//                     )+
//                 }
//            }
//        }

// // $(
// //     // 支持Topology enum自动转换成具体的类型
// //     impl<P> std::ops::Deref for Topology<P> {
// //         type Target = $top;
// //         fn deref(&self) -> &Self::Target {
// //             match self {
// //                 Self::$item(t) => t,
// //                 // 如果有多个实现，把该注释去掉
// //                 //_ => panic!("topology {} not matched", stringify!($top)),
// //             }
// //         }
// //     }
// // )+
// >>>>>>> redis_conn_manage

impl<P> std::ops::Deref for Topology<P> {
    type Target = ServiceTopology<P>;
    fn deref(&self) -> &ServiceTopology<P> {
        match self {
            Self::RedisService(r) => r.clone(),
            Self::CacheService(c) => c.clone(),
        }
    }
}

impl<P> discovery::TopologyWrite for Topology<P>
where
    P: Sync + Send + Protocol,
{
    fn update(&mut self, name: &str, cfg: &str) {
        match self {
            Self::RedisService(r) => discovery::TopologyWrite::update(r, name, cfg),
            Self::CacheService(c) => discovery::TopologyWrite::update(c, name, cfg),
        }
    }

    fn gc(&mut self) {
        match self {
            Self::RedisService(r) => discovery::TopologyWrite::gc(r),
            Self::CacheService(c) => discovery::TopologyWrite::gc(c),
        }
    }
}

pub enum Endpoint<P> {
    RedisService { redis_service: RedisService<P> },
    CacheService { cache_service: CacheService<P> },
}

impl<P> Endpoint<P> {
    pub async fn from_discovery<D>(name: &str, p: P, discovery: D) -> Result<Option<Self>>
    where
        D: TopologyRead<Topology<P>> + Unpin + 'static,
        P: protocol::Protocol,
    {
        match name {
            "rs" => Ok(Some(Self::RedisService {
                redis_service: RedisService::from_discovery(p, discovery).await?,
            })),
            "cs" => Ok(Some(Self::CacheService {
                cache_service: CacheService::from_discovery(p, discovery).await?,
            })),
            _ => Ok(None),
        }
    }
}

impl<P> AsyncReadAll for Endpoint<P>
where
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        match &mut *self {
            Self::RedisService {
                ref mut redis_service,
            } => Pin::new(redis_service).poll_next(cx),
            Self::CacheService {
                ref mut cache_service,
            } => Pin::new(cache_service).poll_next(cx),
            //Endpoint::RedisService {ref mut p} => Pin::new(p).poll_next(cx),
        }
    }
}

impl<P> AsyncWriteAll for Endpoint<P>
where
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        match &mut *self {
            Self::RedisService {
                ref mut redis_service,
            } => Pin::new(redis_service).poll_write(cx, buf),
            Self::CacheService {
                ref mut cache_service,
            } => Pin::new(cache_service).poll_write(cx, buf),
            //Endpoint::RedisService {ref mut p} => Pin::new(p).poll_next(cx),
        }
    }
}
mod cacheservice;
mod redisservice;
mod topology;
//mod pipe;
//mod pipe;

// use cacheservice::CacheService;
// use redisservice::RedisService;
//use pipe::{Pipe, PipeTopology};

// define_endpoint! {
// //    PipeTopology, Pipe,         Pipe,         "pipe";
//     cacheservice::Topology<P>, CacheService, CacheService, "cs";
//     redisservice::Topology<P>, RedisService, RedisService, "rs"
// }

pub trait ServiceTopo {
    fn hash(&self) -> &str;
    fn distribution(&self) -> &str;
    fn listen_ports(&self) -> Vec<u16> {
        vec![]
    }
    fn topo_inited(&self) -> bool;

    fn get(
        &self,
    ) -> (
        Vec<(LayerRole, Vec<BackendStream>)>,
        Vec<(LayerRole, Vec<BackendStream>)>,
    );
    fn mget(
        &self,
    ) -> (
        Vec<(LayerRole, Vec<BackendStream>)>,
        Vec<(LayerRole, Vec<BackendStream>)>,
    );
    // fn get(&mut self) -> Vec<(LayerRole, Vec<BackendStream>)>;
    // fn mget(&mut self) -> Vec<(LayerRole, Vec<BackendStream>)>;
    // fn shared(&self) -> Option<&HashMap<String, Arc<BackendBuilder>>>;

    fn master(&self) -> Vec<BackendStream>;
    fn followers(&self) -> Vec<(LayerRole, Vec<BackendStream>)> {
        vec![]
    }
}
