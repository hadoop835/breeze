pub(crate) mod buffer;
pub(crate) mod handler;
pub mod io;
pub mod request;
pub mod response;
pub use protocol::CallbackContext;
//pub use request::*;
pub use protocol::RequestContext;
pub type Endpoint = MpmcStream<RequestContext>;
pub use response::*;
pub mod pipeline;
mod shards;
pub use shards::*;

mod mpmc;
pub use mpmc::MpmcStream;
pub trait Notify {
    fn notify(&self);
}

pub trait Read {
    fn consume<Out, C: Fn(&[u8]) -> (usize, Out)>(&mut self, c: C) -> Out;
}

mod builder;
pub use builder::BackendBuilder as Builder;
pub use builder::*;

pub(crate) mod checker;
pub(crate) mod timeout;
