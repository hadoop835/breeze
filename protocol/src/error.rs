#[derive(Debug)]
pub enum Error {
    EOF,
    NotInit,
    Closed,
    QueueFull,
    ProtocolNotValid,
    ProtocolNotSupported,
    IO(std::io::Error),
}

impl From<std::io::Error> for Error {
    #[inline(always)]
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl std::error::Error for Error {}
use std::fmt::{self, Display, Formatter};
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error: {:?}", self)
    }
}
