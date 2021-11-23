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

pub struct Flag {
    v: u64,
}
impl Flag {
    #[inline(always)]
    pub fn from_op(op: u8) -> Self {
        Self {
            v: op as u64, // op是第一个字节
        }
    }
    #[inline(always)]
    pub fn new() -> Self {
        Self { v: 0 }
    }
    // 低位第一个字节是operation位
    const STATUS_OK: u8 = 8;
    const SEND_ONLY: u8 = 9;
    #[inline(always)]
    pub fn status_ok(&mut self) -> &mut Self {
        self.mark(Self::STATUS_OK);
        self
    }
    #[inline(always)]
    pub fn is_status_ok(&self) -> bool {
        self.marked(Self::STATUS_OK)
    }
    #[inline(always)]
    pub fn sentonly(&mut self) -> &mut Self {
        self.mark(Self::SEND_ONLY);
        self
    }
    #[inline(always)]
    pub fn is_sentonly(&self) -> bool {
        self.marked(Self::SEND_ONLY)
    }
    #[inline(always)]
    pub fn operation(&mut self, op: Operation) -> &mut Self {
        self.v |= op as u64;
        self
    }
    #[inline(always)]
    pub fn get_operation(&self) -> Operation {
        (self.v as u8).into()
    }

    #[inline(always)]
    pub fn mark(&mut self, bit: u8) {
        self.v |= 1 << bit;
    }
    #[inline(always)]
    pub fn marked(&self, bit: u8) -> bool {
        let m = 1 << bit;
        self.v & m == m
    }
}

pub struct Command {
    flag: Flag,
    cmd: ds::MemGuard,
}

pub struct HashedCommand {
    hash: u64,
    cmd: Command,
}

impl Command {
    #[inline(always)]
    pub fn flag(&self) -> &Flag {
        &self.flag
    }
    #[inline(always)]
    pub fn new(flag: Flag, cmd: ds::MemGuard) -> Self {
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
    pub fn new(cmd: MemGuard, hash: u64, flag: Flag) -> Self {
        Self {
            hash,
            cmd: Command { flag, cmd },
        }
    }
    #[inline(always)]
    pub fn hash(&self) -> u64 {
        self.hash
    }
}
impl AsRef<Command> for HashedCommand {
    #[inline(always)]
    fn as_ref(&self) -> &Command {
        &self.cmd
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for HashedCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "hash:{}  cmd:{}", self.hash, self.cmd)
    }
}
impl Display for Command {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "flag:{} len:{}", self.flag.v, self.len())
    }
}
impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "flag:{} len:{}", self.flag.v, self.len())
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
