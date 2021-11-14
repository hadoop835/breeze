use discovery::TopologyWrite;
use protocol::endpoint::Endpoint;
use protocol::topo::Topology;
use protocol::{Builder, Command, Operation, Protocol, Request, Resource};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use stream::Shards;

#[derive(Clone)]
pub struct CacheService<B, E, Req, P> {
    // 一共有n组，每组1个连接。
    // 排列顺序：master, master l1, slave, slave l1
    streams: Vec<Shards<E, Req>>,
    // streams里面的前r_num个数据是提供读的。
    r_num: u16,
    r_idx: Arc<AtomicUsize>,
    hasher: String,
    parser: P,
    sigs: u64,
    gc: Vec<E>,
    _marker: std::marker::PhantomData<(B, E, Req)>,
}

impl<B, E, Req, P> From<P> for CacheService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            streams: Vec::new(),
            sigs: 0,
            r_num: 0,
            hasher: Default::default(),
            r_idx: Default::default(),
            gc: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<B, E, Req, P> discovery::Inited for CacheService<B, E, Req, P>
where
    E: discovery::Inited,
{
    #[inline]
    fn inited(&self) -> bool {
        self.streams.len() > 0
            && self
                .streams
                .iter()
                .fold(true, |inited, e| inited && e.inited())
    }
}

impl<B, E, Req, P> Topology<Shards<E, Req>> for CacheService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    #[inline]
    fn hasher(&self) -> &str {
        &self.hasher
    }
}

impl<B, E, Req, P> protocol::Endpoint for CacheService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline(always)]
    fn send(&self, req: Self::Item) {
        debug_assert!(self.r_num > 0);
        let idx = if req.operation() == Operation::Store {
            0
        } else {
            self.r_idx.fetch_add(1, Ordering::Relaxed) % self.r_num as usize
        };
        debug_assert!(idx < self.streams.len());
        unsafe { self.streams.get_unchecked(idx).send(req) };
    }
}
impl<B, E, Req, P> TopologyWrite for CacheService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn update(&mut self, name: &str, cfg: &str) {
        let idx = name.find(':').unwrap_or(name.len());
        if idx == 0 || idx >= name.len() - 1 {
            log::info!("not a valid cache service name:{} no namespace found", name);
            return;
        }
        let namespace = &name[idx + 1..];

        super::config::Namespace::parse(self.sigs, cfg, namespace, |sigs, ns| {
            log::info!("cfg changed from {} to {}.", self.sigs, sigs,);
            if ns.master.len() == 0 {
                log::info!("cache service master empty. namespace:{}", namespace);
                return;
            }
            self.hasher = ns.hash;
            let dist = &ns.distribution;

            let old_streams = self.streams.split_off(0);
            self.streams.reserve(old_streams.len());
            // 把streams按address进行flatten
            let mut streams = HashMap::with_capacity(old_streams.len() * 8);
            let old = &mut streams;

            for shards in old_streams {
                let group: Vec<(E, String)> = shards.into();
                for e in group {
                    old.insert(e.1, e.0);
                }
            }
            // 准备master
            let master = self.build(old, ns.master, dist, name);
            self.streams.push(master);

            // master_l1
            for l1 in ns.master_l1 {
                let g = self.build(old, l1, dist, name);
                self.streams.push(g);
            }
            // slave
            if ns.slave.len() > 0 {
                let s = self.build(old, ns.slave, dist, name);
                self.streams.push(s);
            }
            self.r_num = self.streams.len() as u16;
            for sl1 in ns.slave_l1 {
                let g = self.build(old, sl1, dist, name);
                self.streams.push(g);
            }
            self.gc.reserve(old.len());
            for e in streams.into_values() {
                self.gc.push(e);
            }
        });
    }
    fn gc(&mut self) {
        self.gc.clear();
        log::warn!("每次gc都会触发copy on write。copy是一个重型操作。");
    }
}
impl<B, E, Req, P> CacheService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    fn build(
        &self,
        old: &mut HashMap<String, E>,
        addrs: Vec<String>,
        dist: &str,
        name: &str,
    ) -> Shards<E, Req> {
        Shards::from(dist, addrs, |addr| {
            old.remove(addr)
                .map(|e| e)
                .unwrap_or_else(|| B::build(addr, self.parser.clone(), Resource::Memcache, name))
        })
    }
}
