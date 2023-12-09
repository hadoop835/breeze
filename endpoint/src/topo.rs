use discovery::{Inited, TopologyWrite};
use protocol::{Protocol, Request, ResOption, Resource};
use sharding::hash::{Hash, HashKey};

use crate::Timeout;

pub type TopologyProtocol<B, E, R, P> = Topologies<B, E, R, P>;

// 1. 生成一个try_from(parser, endpoint)的方法，endpoint是名字的第一个单词或者是所有单词的首字母。RedisService的名字为"rs"或者"redis"
// 2. trait => where表示，为Topologies实现trait，满足where的条件
// 3. 如果trait是pub的，则同时会创建这个trait。非pub的trait，只会为Topologies实现
procs::topology_dispatcher! {
    #[derive(Clone)]
    pub enum Topologies<B, E, R, P> {
        MsgQue(crate::msgque::topo::MsgQue<B, E, R, P>),
        RedisService(crate::redisservice::topo::RedisService<B, E, R, P>),
        CacheService(crate::cacheservice::topo::CacheService<B, E, R, P>),
        PhantomService(crate::phantomservice::topo::PhantomService<B, E, R, P>),
        KvService(crate::kv::topo::KvService<B, E, R, P>),
        UuidService(crate::uuid::topo::UuidService<B, E, R, P>),
    }

    // #[procs::dispatcher_trait_deref]
    pub trait Endpoint: Sized + Send + Sync {
        type Item;
        fn send(&self, req: Self::Item);
        fn shard_idx(&self, _hash: i64) -> usize {todo!("shard_idx not implemented");}
        fn available(&self) -> bool {todo!("available not implemented");}
        fn addr(&self) -> &str {todo!("addr not implemented");}
    } => where P:Sync+Send+Protocol, E:Endpoint<Item = R> + Inited, R: Request, P: Protocol+Sync+Send, B:Send+Sync

    pub trait Topology : Endpoint + Hash{
        fn exp_sec(&self) -> u32 {86400}
    } => where P:Sync+Send+Protocol, E:Endpoint<Item = R>, R:Request, B: Send + Sync, Topologies<B, E, R, P>: Endpoint

    trait Inited {
        fn inited(&self) -> bool;
    } => where E:Inited

    trait TopologyWrite {
        fn update(&mut self, name: &str, cfg: &str);
        fn disgroup<'a>(&self, _path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)>;
        fn need_load(&self) -> bool;
        fn load(&mut self);
    } => where P:Sync+Send+Protocol, B:Builder<P, R, E>, E:Endpoint<Item = R>+Single

    trait Hash {
        fn hash<S: HashKey>(&self, key: &S) -> i64;
    } => where P:Sync+Send+Protocol, E:Endpoint<Item = R>, R:Request, B:Send+Sync

}

#[procs::dispatcher_trait_deref]
pub trait Single {
    fn single(&self) -> bool;
    fn disable_single(&self);
    fn enable_single(&self);
}

impl<T: Endpoint<Item = R>, R> Endpoint for (String, T) {
    type Item = R;
    #[inline(always)]
    fn send(&self, req: Self::Item) {
        self.1.send(req)
    }
    #[inline(always)]
    fn shard_idx(&self, hash: i64) -> usize {
        self.1.shard_idx(hash)
    }
    #[inline(always)]
    fn available(&self) -> bool {
        self.1.available()
    }
}

pub trait Builder<P, R, E> {
    fn build(addr: &str, parser: P, rsrc: Resource, service: &str, timeout: Timeout) -> E {
        Self::auth_option_build(addr, parser, rsrc, service, timeout, Default::default())
    }

    // TODO: ResOption -> AuthOption
    fn auth_option_build(
        addr: &str,
        parser: P,
        rsrc: Resource,
        service: &str,
        timeout: Timeout,
        option: ResOption,
    ) -> E;
}

// 从环境变量获取是否开启后端资源访问的性能模式
#[inline]
fn is_performance_tuning_from_env() -> bool {
    context::get().timeslice()
}

pub(crate) trait PerformanceTuning {
    fn tuning_mode(&self) -> bool;
}

impl PerformanceTuning for String {
    fn tuning_mode(&self) -> bool {
        is_performance_tuning_from_env()
            || match self.as_str() {
                "distance" | "timeslice" => true,
                _ => false,
            }
    }
}

impl PerformanceTuning for bool {
    fn tuning_mode(&self) -> bool {
        is_performance_tuning_from_env() || *self
    }
}

pub struct Pair<E> {
    pub addr: String,
    pub endpoint: E,
}
impl<E: Endpoint> From<(String, E)> for Pair<E> {
    fn from(pair: (String, E)) -> Pair<E> {
        Pair {
            addr: pair.0,
            endpoint: pair.1,
        }
    }
}
impl<E: Endpoint> From<E> for Pair<E> {
    fn from(pair: E) -> Pair<E> {
        Pair {
            addr: pair.addr().to_string(),
            endpoint: pair,
        }
    }
}

use std::collections::HashMap;
pub struct Endpoints<'a, B, R, P, E: Endpoint> {
    service: &'a str,
    parser: &'a P,
    resource: Resource,
    cache: HashMap<String, Vec<E>>,
    _marker: std::marker::PhantomData<(B, R)>,
}
impl<'a, B, R, P, E: Endpoint> Endpoints<'a, B, R, P, E> {
    pub fn new(service: &'a str, parser: &'a P, resource: Resource) -> Self {
        Endpoints {
            service,
            parser,
            resource,
            cache: HashMap::new(),
            _marker: Default::default(),
        }
    }
    pub fn cache<T: Into<Pair<E>>>(&mut self, endpoints: Vec<T>) {
        self.cache.reserve(endpoints.len());
        for pair in endpoints.into_iter().map(|e| e.into()) {
            self.cache
                .entry(pair.addr)
                .or_insert(Vec::new())
                .push(pair.endpoint);
        }
    }
    pub fn with_cache<T: Into<Pair<E>>>(mut self, endpoints: Vec<T>) -> Self {
        self.cache(endpoints);
        self
    }
}

impl<'a, B: Builder<P, R, E>, R, P: Protocol, E: Endpoint> Endpoints<'a, B, R, P, E> {
    pub fn take_or_build_one(&mut self, addr: &str, to: Timeout) -> E {
        self.take_or_build(&[addr.to_owned()], to)
            .pop()
            .expect("take")
            .1
    }
    pub fn take_or_build(&mut self, addrs: &[String], to: Timeout) -> Vec<(String, E)> {
        addrs
            .iter()
            .map(|addr| {
                let a = addr.to_string();
                let e = self
                    .cache
                    .get_mut(addr)
                    .map(|endpoints| endpoints.pop())
                    .flatten()
                    .unwrap_or_else(|| {
                        B::build(&addr, self.parser.clone(), self.resource, self.service, to)
                    });
                (a, e)
            })
            .collect()
    }
}
// 为Endpoints实现Formatter
impl<'a, B, R, P, E: Endpoint> std::fmt::Display for Endpoints<'a, B, R, P, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        for (addr, endpoints) in self.cache.iter() {
            s.push_str(addr);
            s.push_str(" ");
            s.push_str(&endpoints.len().to_string());
            s.push_str("\n");
        }
        write!(f, "{}", s)
    }
}

// 为Endpoints实现Drop
impl<'a, B, R, P, E: Endpoint> Drop for Endpoints<'a, B, R, P, E> {
    fn drop(&mut self) {
        if self.cache.len() > 0 {
            log::info!("drop endpoints:{}", self);
        }
    }
}
