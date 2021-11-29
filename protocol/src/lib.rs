pub mod memcache;
pub mod proto;
pub mod req;
pub mod resp;
pub mod topo;

pub use proto::Proto as Protocol;
pub use proto::*;
pub use topo::*;

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

impl ResponseWriter for Vec<u8> {
    #[inline(always)]
    fn write(&mut self, data: &[u8]) -> Result<()> {
        ds::vec::Buffer::write(self, data);
        Ok(())
    }
}
