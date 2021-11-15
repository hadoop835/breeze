use enum_dispatch::enum_dispatch;

use sharding::hash::Hash;

use crate::{Error, Operation, Result};

pub trait RequestProcessor {
    fn process(&mut self, req: HashedCommand);
}

pub trait Stream {
    fn len(&self) -> usize;
    fn at(&self, idx: usize) -> u8;
    fn slice(&self) -> ds::RingSlice;
    fn update(&mut self, idx: usize, val: u8);
    fn take(&mut self, n: usize) -> ds::MemGuard;
}

pub trait Builder {
    type Endpoint: crate::endpoint::Endpoint;
    fn build(&self) -> Self::Endpoint;
}

pub struct Command {
    flag: u64,
    cmd: ds::MemGuard,
}

pub struct HashedCommand {
    operation: Operation,
    sentonly: bool,
    hash: u64,
    cmd: Command,
}

impl Command {
    #[inline(always)]
    pub fn flag(&self) -> u64 {
        self.flag
    }
    #[inline(always)]
    pub fn new(flag: u64, cmd: ds::MemGuard) -> Self {
        Self { flag, cmd }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.cmd.len()
    }
    #[inline(always)]
    pub fn read(&self, oft: usize) -> &[u8] {
        self.cmd.read(oft)
    }
}

impl std::ops::Deref for HashedCommand {
    type Target = Command;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}
use ds::MemGuard;
impl HashedCommand {
    #[inline(always)]
    pub fn new(cmd: MemGuard, hash: u64, flag: u64, sentonly: bool, op: Operation) -> Self {
        Self {
            operation: op,
            sentonly: sentonly,
            hash,
            cmd: Command { flag, cmd },
        }
    }
    #[inline(always)]
    pub fn sentonly(&self) -> bool {
        self.sentonly
    }
    #[inline(always)]
    pub fn hash(&self) -> u64 {
        self.hash
    }
    #[inline(always)]
    pub fn operation(&self) -> Operation {
        self.operation
    }
}
impl AsRef<Command> for HashedCommand {
    #[inline(always)]
    fn as_ref(&self) -> &Command {
        &self.cmd
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for HashedCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "op:{} hash:{} sentonly:{} flag:{} len:{}",
            self.operation as u8,
            self.hash,
            self.sentonly,
            self.flag(),
            self.len()
        )
    }
}
impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "flag:{} len:{}", self.flag(), self.len())
    }
}
use crate::memcache::MemcacheBinary;
#[enum_dispatch(Proto)]
#[derive(Clone)]
pub enum Parser {
    McBin(MemcacheBinary),
}
impl Parser {
    pub fn try_from(name: &str) -> Result<Self> {
        match name {
            "mc" => Ok(Self::McBin(MemcacheBinary::default())),
            _ => Err(Error::ProtocolNotValid),
        }
    }
}
#[enum_dispatch]
pub trait Proto: Unpin + Clone + Send + Sync + 'static {
    // params:
    // data: 是请求数据。调用方确保OwnedRingSlice持有的数据没有其他slice引用。
    // alg: hash算法。
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()>;
    fn operation<C: AsRef<Command>>(&self, cmd: C) -> Operation;
    fn parse_response<S: Stream>(&self, data: &mut S) -> Option<Command>;
    fn ok(&self, cmd: &Command) -> bool;
    fn write_response<W: crate::ResponseWriter>(
        &self,
        req: &HashedCommand,
        resp: &Command,
        w: &mut W,
    ) -> Result<()>;
}
