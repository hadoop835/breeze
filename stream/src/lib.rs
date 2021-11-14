pub(crate) mod buffer;
mod callback;
pub(crate) mod handler;
pub mod io;
pub mod request;
pub mod response;
pub use callback::*;
//pub use request::*;
pub use request::Request;
pub type Endpoint = MpmcStream<Request>;
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
