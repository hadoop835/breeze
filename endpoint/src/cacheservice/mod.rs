mod config;
mod topology;
use config::Namespace;

pub use topology::Topology;

use discovery::TopologyRead;
use stream::backend::AddressEnable;

use std::collections::HashMap;

use stream::{
    AsyncLayerGet, AsyncMultiGetSharding, AsyncOpRoute, AsyncOperation, AsyncSetSync,
    AsyncSharding, MetaStream,
};

use protocol::Protocol;

use std::io::Result;

type Backend = stream::BackendStream;

// type GetOperation<P> = AsyncSharding<Backend, Hasher, P>;
type GetOperation<P> = AsyncLayerGet<AsyncSharding<Backend, P>, P>;

type MultiGetLayer<P> = AsyncMultiGetSharding<Backend, P>;
type MultiGetOperation<P> = AsyncLayerGet<MultiGetLayer<P>, P>;

type Master<P> = AsyncSharding<Backend, P>;
type Follower<P> = AsyncSharding<Backend, P>;
type StoreOperation<P> = AsyncSetSync<Master<P>, Follower<P>, P>;
type MetaOperation<P> = MetaStream<P, Backend>;
type Operation<P> =
    AsyncOperation<GetOperation<P>, MultiGetOperation<P>, StoreOperation<P>, MetaOperation<P>>;

// 三级访问策略。
// 第一级先进行读写分离
// 第二级按key进行hash
// 第三级进行pipeline与server进行交互
pub struct CacheService<P> {
    inner: AsyncOpRoute<Operation<P>>,
}

impl<P> CacheService<P> {
    #[inline]
    fn build_sharding<S>(
        shards: Vec<S>,
        h: &str,
        distribution: &str,
        parser: P,
    ) -> AsyncSharding<S, P>
    where
        S: AsyncWriteAll + AddressEnable,
    {
        AsyncSharding::from(shards, h, distribution, parser)
    }

    #[inline]
    fn build_get_layers<S>(
        pools: Vec<Vec<S>>,
        h: &str,
        distribution: &String,
        parser: P,
    ) -> Vec<AsyncSharding<S, P>>
    where
        S: AsyncWriteAll + AddressEnable,
        P: Protocol + Clone,
    {
        let mut layers: Vec<AsyncSharding<S, P>> = Vec::new();
        for p in pools {
            layers.push(AsyncSharding::from(p, h, distribution, parser.clone()));
        }
        layers
    }

    #[inline]
    fn build_get_multi_layers<S>(
        pools: Vec<Vec<S>>,
        parser: P,
        h: &str,
        d: &str,
    ) -> Vec<AsyncMultiGetSharding<S, P>>
    where
        S: AsyncWriteAll + AddressEnable,
        P: Clone,
    {
        let mut layers: Vec<AsyncMultiGetSharding<S, P>> = Vec::new();
        for p in pools {
            layers.push(AsyncMultiGetSharding::from_shard(p, parser.clone(), h, d));
        }
        layers
    }

    pub async fn from_discovery<D>(p: P, discovery: D) -> Result<Self>
    where
        D: TopologyRead<super::Topology<P>>,
        P: protocol::Protocol,
    {
        discovery.do_with(|t| Self::from_topology::<D>(p.clone(), t))
    }
    fn from_topology<D>(parser: P, topo: &Topology<P>) -> Result<Self>
    where
        D: TopologyRead<super::Topology<P>>,
        P: protocol::Protocol,
    {
        let hash_alg = &topo.hash;
        let distribution = &topo.distribution;
        let get_multi_layers = Self::build_get_multi_layers(
            topo.retrive_gets(),
            parser.clone(),
            hash_alg,
            distribution,
        );
        let get_multi =
            AsyncOperation::Gets(AsyncLayerGet::from_layers(get_multi_layers, parser.clone()));

        let master = Self::build_sharding(topo.master(), &hash_alg, distribution, parser.clone());
        let followers = topo
            .followers()
            .into_iter()
            .map(|shards| Self::build_sharding(shards, &hash_alg, distribution, parser.clone()))
            .collect();
        let store =
            AsyncOperation::Store(AsyncSetSync::from_master(master, followers, parser.clone()));

        // 获取get through
        let get_layers =
            Self::build_get_layers(topo.retrive_get(), &hash_alg, distribution, parser.clone());
        let get = AsyncOperation::Get(AsyncLayerGet::from_layers(get_layers, parser.clone()));

        let all_instances = topo.meta();

        let meta = AsyncOperation::Meta(MetaStream::from(parser.clone(), all_instances));
        let mut operations = HashMap::with_capacity(4);
        operations.insert(protocol::Operation::Get, get);
        operations.insert(protocol::Operation::Gets, get_multi);
        operations.insert(protocol::Operation::Store, store);
        operations.insert(protocol::Operation::Meta, meta);
        let op_stream = AsyncOpRoute::from(operations);

        Ok(Self { inner: op_stream })
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};

use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};

impl<P> AsyncReadAll for CacheService<P>
where
    P: Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<P> AsyncWriteAll for CacheService<P>
where
    P: Protocol,
{
    // 支持pipelin.
    // left是表示当前请求还有多少个字节未写入完成
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &Request,
    ) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
}
