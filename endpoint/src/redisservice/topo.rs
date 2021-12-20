use discovery::TopologyWrite;
use protocol::{Builder, Endpoint, Protocol, Request, Resource, Topology};
use sharding::hash::Hasher;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use stream::Shards;
#[derive(Clone)]
pub struct RedisService<B, E, Req, P> {
    // 一共shards.len()个分片，每个分片 shard[0]是master, shard[1..]是slave
    shards: Vec<Vec<E>>,
    r_idx: Vec<AtomicUsize>, // 选择哪个从读取数据
    // 不同sharding的url。第0个是master
    shards_url: Vec<Vec<String>>,
    _mark: std::marker::PhantomData<(B, E, Req, P)>,
}
impl<B, E, Req, P> Topology for RedisService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    #[inline]
    fn hasher(&self) -> &Hasher {
        todo!();
    }
}

impl<B: Send + Sync, E, Req, P> protocol::Endpoint for RedisService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline(always)]
    fn send(&self, mut req: Self::Item) {
        debug_assert_ne!(self.shards.len(), 0);
        let shard_idx = req.hash() % self.shards.len();
        let shard = unsafe { self.shards.get_unchecked(shard_idx) };
        let mut idx = 0;
        // 如果有从，并且是读请求。
        if shard.len() >= 2 && !req.operation().is_store() {
            debug_assert_eq!(self.shards.len() == self.r_idx.len());
            let r_idx_seq = self
                .r_idx
                .get_unchecked(shard_idx)
                .fetch_add(1, Ordering::Relaxed);
            idx = r_idx_seq % (self.shards.len() - 1) + 1;
        }
        shard[idx].send(req);
    }
}
impl<B, E, Req, P> From<P> for RedisService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        todo!();
    }
}
impl<B, E, Req, P> TopologyWrite for RedisService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        todo!();
    }
}
impl<B, E, Req, P> discovery::Inited for RedisService<B, E, Req, P>
where
    E: discovery::Inited,
{
    #[inline]
    fn inited(&self) -> bool {
        todo!();
    }
}
