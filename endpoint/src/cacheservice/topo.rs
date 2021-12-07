use discovery::TopologyWrite;
use protocol::{Builder, Endpoint, Operation, Protocol, Request, Resource, Topology};
use sharding::hash::Hasher;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use stream::Shards;

#[derive(Clone)]
pub struct CacheService<B, E, Req, P> {
    // 一共有n组，每组1个连接。
    // 排列顺序： master, master l1, slave, slave l1
    streams: Vec<Shards<E, Req>>,
    // streams里面的前r_num个数据是提供读的(这个长度不包含slave l1)。
    r_num: u16,
    rnd_idx: Arc<AtomicUsize>, // 读请求时，随机读取
    has_l1: bool,              // 是否包含masterl1
    has_slave: bool,
    hasher: Hasher,
    parser: P,
    sigs: u64,
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
            has_l1: false,
            has_slave: false,
            hasher: Default::default(),
            rnd_idx: Default::default(),
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

impl<B, E, Req, P> Topology for CacheService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    #[inline]
    fn hasher(&self) -> &Hasher {
        &self.hasher
    }
}

impl<B, E, Req, P> protocol::Endpoint for CacheService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    type Item = Req;
    #[inline(always)]
    fn send(&self, mut req: Self::Item) {
        log::debug!("sending req:{} {} ", req, self,);
        debug_assert!(self.r_num > 0);
        let mut ctx = super::Context::from(*req.mut_context());
        let idx: usize;
        let goon: bool;
        if req.operation() == Operation::Store {
            // 有3种情况。
            // s1: 第一次访问，则直接写主
            // s2: 是回种访问，则回种的索引都在ctx中。
            // s3: 同步写，则idx + 1
            ctx.check_and_inited(true);
            // 第一次过来。
            if ctx.is_write() {
                req.write_back(self.streams.len() > 1); // 除了主还有其他的需要write back
                idx = ctx.take_write_idx() as usize;
                log::debug!("is write operation:{}", idx);
                goon = idx + 1 < self.streams.len();
            } else {
                idx = ctx.take_read_idx() as usize;
                // 当前是master，并且有master l1，说明需要继续回写
                goon = idx == 0 && self.has_l1 && idx < self.r_num as usize;
            };
            req.goon(goon);
            if idx as usize >= self.streams.len() {
                // 在出现top变更的时候，可能会导致ctx存储的值发生变化
                req.on_err(protocol::Error::IndexOutofBound);
                return;
            }
        } else {
            if !ctx.check_and_inited(false) {
                idx = self.rnd_idx.fetch_add(1, Ordering::Relaxed) % self.r_num as usize;
                // 第一次访问，没有取到master，则下一次一定可以取到master
                // 如果取到了master，有slave也可以继续访问
                goon = idx != 0 || self.has_slave;
            } else {
                // 不是第一次访问，获取上一次访问的index
                let last_idx = ctx.index();
                if last_idx != 0 {
                    idx = 0;
                    goon = self.has_slave;
                } else {
                    // 上一次取到了master，当前取slave
                    idx = self.r_num as usize - 1;
                    goon = false;
                }
                req.write_back(true);
            }
            req.goon(goon);
            // 把当前访问过的idx记录到ctx中，方便回写时使用。
            ctx.write_back_idx(idx as u16);
        };
        *req.mut_context() = ctx.ctx;
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
            self.hasher = Hasher::from(&ns.hash);
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
            self.has_l1 = ns.master_l1.len() > 0;
            for l1 in ns.master_l1 {
                let g = self.build(old, l1, dist, name);
                self.streams.push(g);
            }

            // slave
            self.has_slave = ns.slave.len() > 0;
            if ns.slave.len() > 0 {
                let s = self.build(old, ns.slave, dist, name);
                self.streams.push(s);
            }
            self.r_num = self.streams.len() as u16;
            for sl1 in ns.slave_l1 {
                let g = self.build(old, sl1, dist, name);
                self.streams.push(g);
            }
            // old 会被dopped
        });
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

use std::fmt::{self, Display, Formatter};
impl<B, E, Req, P> Display for CacheService<B, E, Req, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shards:{} r-shards:{} has master l1:{} has slave:{} l1 rand idx:{}",
            self.streams.len(),
            self.r_num,
            self.has_l1,
            self.has_slave,
            self.rnd_idx.load(Ordering::Relaxed),
        )
    }
}
