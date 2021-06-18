mod update;
mod vintage;
use update::AsyncServiceUpdate;
use vintage::Vintage;

use std::io::Result;
use std::time::Duration;

use left_right::ReadHandle;

use url::Url;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

#[async_trait]
#[enum_dispatch]
pub trait Discover {
    async fn get_service(&self, name: &str, sig: &str) -> Result<Option<(String, String)>>;
}

#[enum_dispatch(Discover)]
pub enum Discovery {
    Vintage(Vintage),
}
impl Discovery {
    pub fn from_url(url: Url) -> Self {
        match url.scheme() {
            "vintage" => Self::Vintage(Vintage::from_url(url)),
            _ => panic!("not supported endpoint name"),
        }
    }
}

pub trait Topology: Default + left_right::Absorb<String> + Clone {
    fn update(&mut self, cfg: &str);
}

pub trait ServiceDiscover<T> {
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O;
}

unsafe impl<T> Send for ServiceDiscovery<T> {}
unsafe impl<T> Sync for ServiceDiscovery<T> {}

pub struct ServiceDiscovery<T> {
    cache: ReadHandle<T>,
}

impl<T> ServiceDiscovery<T> {
    pub fn new<D>(discovery: D, service: String, snapshot: String, tick: Duration) -> Self
    where
        D: Discover + Send + Unpin + 'static,
        T: Topology + Send + Sync + 'static,
    {
        let (w, r) = left_right::new_from_empty::<T, String>(T::default());

        tokio::spawn(AsyncServiceUpdate::new(
            service, discovery, w, tick, snapshot,
        ));

        Self { cache: r }
    }
}

impl<T> ServiceDiscover<T> for ServiceDiscovery<T> {
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        f(&self.cache.enter().expect("topology not inited yes"))
    }
}

use std::sync::Arc;

#[async_trait]
impl<T: Discover + Send + Unpin + Sync> Discover for Arc<T> {
    #[inline]
    async fn get_service(&self, name: &str, sig: &str) -> Result<Option<(String, String)>> {
        (**self).get_service(name, sig).await
    }
}
impl<T> ServiceDiscover<T> for Arc<ServiceDiscovery<T>> {
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        (**self).do_with(f)
    }
}
