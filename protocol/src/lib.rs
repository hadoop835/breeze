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

mod request;
pub use request::*;

mod response;
pub use response::*;

pub trait ResponseWriter {}

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

pub trait Builder<P, E> {
    fn build(addr: &str, parser: P, rsrc: Resource, service: &str) -> E;
}
mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
