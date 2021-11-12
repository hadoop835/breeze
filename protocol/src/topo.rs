use enum_dispatch::enum_dispatch;

use crate::endpoint::Endpoint;

#[enum_dispatch]
pub trait Topology<E: Endpoint>: Endpoint {
    fn hasher(&self) -> &str;
}
