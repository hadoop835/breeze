use crate::Endpoint;
use sharding::distribution::Distribute;

#[derive(Clone)]
pub(crate) struct Shards<E, Req> {
    router: Distribute,
    backends: Vec<E>,
    _mark: std::marker::PhantomData<Req>,
}
impl<E, Req> Endpoint for Shards<E, Req>
where
    E: Endpoint<Item = Req>,
    Req: protocol::Request,
{
    type Item = Req;
    #[inline]
    fn send(&self, req: Req) {
        let idx = self.shard_idx(req.hash());
        assert!(idx < self.backends.len());
        unsafe { self.backends.get_unchecked(idx).send(req) };
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        assert!(self.backends.len() > 0);
        if self.backends.len() > 1 {
            self.router.index(hash)
        } else {
            0
        }
    }
    fn available(&self) -> bool {
        true
    }
}

use discovery::Inited;
impl<E: Inited, Req> Inited for Shards<E, Req> {
    fn inited(&self) -> bool {
        self.backends.len() > 0
            && self
                .backends
                .iter()
                .fold(true, |inited, e| inited && e.inited())
    }
}

impl<E: Endpoint, Req> Into<Vec<E>> for Shards<E, Req> {
    #[inline]
    fn into(self) -> Vec<E> {
        self.backends
    }
}

impl<E, Req> Shards<E, Req> {
    pub fn from_dist(dist: &str, group: Vec<E>) -> Self
    where
        E: Endpoint,
    {
        let addrs = group.iter().map(|e| e.addr()).collect::<Vec<_>>();
        let router = Distribute::from(dist, &addrs);
        Self {
            router,
            backends: group,
            _mark: Default::default(),
        }
    }
}

use discovery::distance::Addr;
impl<E: Endpoint, Req> Addr for Shards<E, Req> {
    #[inline]
    fn addr(&self) -> &str {
        self.backends.get(0).map(|b| b.addr()).unwrap_or("")
    }
    fn visit(&self, f: &mut dyn FnMut(&str)) {
        self.backends.iter().for_each(|b| f(b.addr()))
    }
}

use crate::select::Distance;
#[derive(Clone)]
pub struct Shard<E> {
    pub(crate) master: E,
    pub(crate) slaves: Distance<E>,
}
impl<E: Endpoint> Shard<E> {
    #[inline]
    pub fn selector(performance: bool, master: E, replicas: Vec<E>, region_enabled: bool) -> Self {
        Self {
            master,
            slaves: Distance::with_mode(replicas, performance, region_enabled),
        }
    }
}
impl<E> Shard<E> {
    #[inline]
    pub(crate) fn has_slave(&self) -> bool {
        self.slaves.len() > 0
    }
    #[inline]
    pub(crate) fn master(&self) -> &E {
        &self.master
    }
    #[inline]
    pub(crate) fn select(&self) -> (usize, &E) {
        self.slaves.unsafe_select()
    }
    #[inline]
    pub(crate) fn next(&self, idx: usize, runs: usize) -> (usize, &E)
    where
        E: Endpoint,
    {
        unsafe { self.slaves.unsafe_next(idx, runs) }
    }
    pub(crate) fn check_region_len(&self, ty: &str, service: &str)
    where
        E: Endpoint,
    {
        use discovery::dns::IPPort;
        let addr = self.master.addr();
        let port = addr.port();
        // TODO: 10000: 这个值是与监控系统共享的，如果修改，需要同时修改监控系统的配置
        let n = self.slaves.len_region().map(|l| l + 10000).unwrap_or(0);
        let f = |r: &str| format!("{}:{}", r, port);
        let bip = context::get()
            .region()
            .map(f)
            .unwrap_or_else(|| f(discovery::distance::host_region().as_str()));

        metrics::resource_num_metric(ty, service, &*bip, n);
    }
}
impl<E: discovery::Inited> Shard<E> {
    // 1. 主已经初始化
    // 2. 有从
    // 3. 所有的从已经初始化
    #[inline]
    pub(crate) fn inited(&self) -> bool {
        self.master().inited()
            && self.has_slave()
            && self
                .slaves
                .iter()
                .fold(true, |inited, e| inited && e.inited())
    }
}
// 为Shard实现Debug
impl<E: Endpoint> std::fmt::Debug for Shard<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard")
            .field("master", &self.master.addr())
            .field("slaves", &self.slaves)
            .finish()
    }
}
