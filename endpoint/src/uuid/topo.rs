use crate::{
    dns::{DnsConfig, DnsLookup},
    select::Distance,
    Builder, Endpoint, Endpoints, PerformanceTuning, Single, Topology,
};
use discovery::TopologyWrite;
use protocol::{Protocol, Request, Resource::Uuid};
use sharding::hash::{Hash, HashKey};

use super::config::UuidNamespace;

#[derive(Clone)]
pub struct UuidService<B, E, Req, P> {
    shard: Distance<(String, E)>,
    parser: P,
    cfg: Box<DnsConfig<UuidNamespace>>,
    _mark: std::marker::PhantomData<(B, Req)>,
}
impl<B, E, Req, P> From<P> for UuidService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            shard: Distance::new(),
            parser,
            cfg: Default::default(),
            _mark: Default::default(),
        }
    }
}

impl<B, E, Req, P> Hash for UuidService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    #[inline]
    fn hash<K: HashKey>(&self, _k: &K) -> i64 {
        0
    }
}

impl<B, E, Req, P> Topology for UuidService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
}

impl<B: Send + Sync, E, Req, P> Endpoint for UuidService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline]
    fn send(&self, mut req: Self::Item) {
        log::debug!("+++ {} send => {:?}", self.cfg.service, req);

        if *req.context_mut() == 0 {
            if let Some(quota) = self.shard.quota() {
                req.quota(quota);
            }
        }

        let ctx = super::transmute(req.context_mut());
        let (idx, endpoint) = if ctx.runs == 0 {
            self.shard.unsafe_select()
        } else {
            unsafe { self.shard.unsafe_next(ctx.idx as usize, ctx.runs as usize) }
        };
        log::debug!("{} =>, idx:{}, addr:{}", self, idx, endpoint.addr(),);

        ctx.idx = idx as u16;
        ctx.runs += 1;

        let try_next = ctx.runs == 1;
        req.try_next(try_next);
        endpoint.send(req);
    }

    #[inline]
    fn shard_idx(&self, _hash: i64) -> usize {
        0
    }
}
impl<B, E, Req, P> TopologyWrite for UuidService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req> + Single,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = UuidNamespace::try_from(cfg) {
            self.cfg.update(namespace, ns);
        }
    }
    #[inline]
    fn need_load(&self) -> bool {
        self.cfg.need_load() || self.shard.len() == 0
    }

    #[inline]
    fn load(&mut self) {
        self.cfg
            .load_guard()
            .check_load(|| self.load_inner().is_some());
    }
}
impl<B, E, Req, P> discovery::Inited for UuidService<B, E, Req, P>
where
    E: discovery::Inited,
{
    #[inline]
    fn inited(&self) -> bool {
        self.shard.len() > 0
            && self
                .shard
                .iter()
                .fold(true, |inited, (_, e)| inited && e.inited())
    }
}

impl<B, E, Req, P> UuidService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req> + Single,
{
    #[inline]
    fn load_inner(&mut self) -> Option<()> {
        let addrs = self.cfg.shards_url.flatten_lookup()?;
        assert_ne!(addrs.len(), 0);
        let mut endpoints: Endpoints<'_, B, Req, P, E> =
            Endpoints::new(&self.cfg.service, &self.parser, Uuid).with_cache(self.shard.take());
        let backends = endpoints.take_or_build(&addrs, self.cfg.timeout());
        self.shard = Distance::with_performance_tuning(
            backends,
            self.cfg.basic.selector.tuning_mode(),
            self.cfg.basic.region_enabled,
        );

        log::info!("{} load backends. dropping:{}", self, endpoints);
        Some(())
    }
}

impl<B, E, Req, P> std::fmt::Display for UuidService<B, E, Req, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UuidService")
            .field("cfg", &self.cfg)
            .field("backends", &self.shard.len())
            .finish()
    }
}
