pub mod endpoint;
pub mod proto;
pub mod req;
pub mod resp;
pub mod topo;

pub mod memcache;
pub use endpoint::*;

pub use proto::Proto as Protocol;
pub use proto::*;

pub use req::*;
mod operation;
pub use operation::*;

pub trait ResponseWriter {
    // 写数据，一次写完
    fn write(&mut self, data: &[u8]) -> Result<()>;
}

#[derive(Copy, Clone)]
pub enum Resource {
    Memcache,
}

impl Resource {
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Memcache => "mc",
        }
    }
}

pub trait Builder<P, R, E> {
    fn build(addr: &str, parser: P, rsrc: Resource, service: &str) -> E
    where
        E: Endpoint<Item = R>;
}
mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
