mod config;
mod topology;
use config::Namespace;

pub use topology::Topology;

use discovery::ServiceDiscover;

use hash::Hasher;
use protocol::chan::{
    AsyncMultiGet, AsyncOperation, AsyncRoute, AsyncSetSync, AsyncSharding, AsyncWriteAll,
    PipeToPingPongChanWrite,
};
use protocol::{MetaStream, Protocol};

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use tokio::net::tcp::OwnedWriteHalf;

use stream::{Cid, RingBufferStream};

type Backend = stream::BackendStream<Arc<RingBufferStream>, Cid>;

type GetOperation<P> = AsyncSharding<Backend, Hasher, P>;
type MultiGetOperation<P> = AsyncMultiGet<Backend, P>;

//type Reader<P> = AsyncRoute<Backend, P>;
//type GetThroughOperation<P> = AsyncGetSync<Reader<P>, P>;

type Master<P> = AsyncSharding<Backend, Hasher, P>;
type Follower<P> = AsyncSharding<OwnedWriteHalf, Hasher, P>;
type StoreOperation<P> = AsyncSetSync<Master<P>, Follower<P>>;
type MetaOperation = MetaStream;
type Operation<P> =
    AsyncOperation<GetOperation<P>, MultiGetOperation<P>, StoreOperation<P>, MetaOperation>;

// 三级访问策略。
// 第一级先进行读写分离
// 第二级按key进行hash
// 第三级进行pipeline与server进行交互
pub struct CacheService<P> {
    inner: PipeToPingPongChanWrite<P, AsyncRoute<Operation<P>, P>>,
}

impl<P> CacheService<P> {
    #[inline]
    fn build_sharding<S>(shards: Vec<S>, h: &str, parser: P) -> AsyncSharding<S, Hasher, P>
    where
        S: AsyncWrite + AsyncWriteAll,
    {
        let hasher = Hasher::from(h);
        AsyncSharding::from(shards, hasher, parser)
    }

    pub async fn from_discovery<D>(p: P, discovery: D) -> Result<Self>
    where
        D: ServiceDiscover<super::Topology<P>>,
        P: protocol::Protocol,
    {
        discovery.do_with(|t| match t {
            Some(t) => match t {
                super::Topology::CacheService(t) => Self::from_topology::<D>(p, t),
                _ => panic!("cacheservice topologyt required!"),
            },
            None => Err(Error::new(
                ErrorKind::ConnectionRefused,
                "backend server not inited yet",
            )),
        })
    }
    fn from_topology<D>(parser: P, topo: &Topology<P>) -> Result<Self>
    where
        D: ServiceDiscover<super::Topology<P>>,
        P: protocol::Protocol,
    {
        let hash_alg = &topo.hash;
        let get = AsyncOperation::Get(Self::build_sharding(
            topo.next_l1(),
            &hash_alg,
            parser.clone(),
        ));

        let l1 = topo.next_l1_gets();
        let gets = AsyncOperation::Gets(AsyncMultiGet::from_shard(l1, parser.clone()));

        let master = Self::build_sharding(topo.master(), &hash_alg, parser.clone());
        let followers = topo
            .followers()
            .into_iter()
            .map(|shards| Self::build_sharding(shards, &hash_alg, parser.clone()))
            .collect();
        let store = AsyncOperation::Store(AsyncSetSync::from_master(master, followers));

        let meta = AsyncOperation::Meta(parser.meta(""));
        let op_stream = AsyncRoute::from(vec![get, gets, store, meta], parser.clone());

        let inner = PipeToPingPongChanWrite::from_stream(parser, op_stream);

        Ok(Self { inner: inner })
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

impl<P> AsyncRead for CacheService<P>
where
    P: Protocol,
{
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<P> AsyncWrite for CacheService<P>
where
    P: Protocol,
{
    // 支持pipelin.
    // left是表示当前请求还有多少个字节未写入完成
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
