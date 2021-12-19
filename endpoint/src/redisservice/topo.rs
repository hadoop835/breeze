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
        todo!();
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
