use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::{interval, Interval};

use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    AsyncResolver, TokioConnection, TokioConnectionProvider, TokioHandle,
};

// 提供一个dns的定期解析器

pub struct DnsResolver {
    resolver: AsyncResolver<TokioConnection, TokioConnectionProvider>,
    hosts: HashMap<String, Record>,
    tick: Interval,
    no_cache: HashMap<String, ()>,
}

impl DnsResolver {
    // 检查dns是否
    pub fn check_update(&self, dns: &str) -> bool {
        todo!();
    }
    pub fn poll_no_cache(&mut self, cx: &mut Context<'_>) -> Poll<()> {}
}

impl Future for DnsResolver {
    type Output = ();

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            // 先把从来没有lookup的域名lookup一次
            self.tick.poll_tick();
        }
    }
}

#[derive(Default)]
struct Record {
    updated: bool,
    ips: Vec<u32>,
}
