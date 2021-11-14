mod flag;
use flag::*;

mod meta;
use meta::*;

use crate::{Error, Operation, Result, Stream};

#[derive(Clone, Default)]
pub struct MemcacheBinary;

use crate::proto::RequestProcessor;
use crate::proto::{Command, HashedCommand};
use sharding::hash::Hash;
impl crate::proto::Proto for MemcacheBinary {
    // 解析请求。把所有的multi-get请求转换成单一的n个get请求。
    #[inline(always)]
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        data: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        debug_assert!(data.len() > 0);
        if data.at(PacketPos::Magic as usize) != REQUEST_MAGIC {
            return Err(Error::ProtocolNotValid);
        }
        log::info!("{} bytes parsing", data.len());
        while data.len() >= HEADER_LEN {
            let req = data.slice();
            let packet_len = req.packet_len();
            log::info!("packet len:{} req len:{}", packet_len, req.len());
            if req.len() < packet_len {
                break;
            }
            // 把quite get请求，转换成单个的get请求
            if req.quite_get() {
                let idx = PacketPos::Opcode as usize;
                data.update(idx, UN_MULT_GETS_OPS[req.op() as usize]);
            }
            let sentonly = NOREPLY_MAPPING[req.op() as usize] == req.op();
            let flag = req.op() as u64;
            let hash = alg.hash(&req.key());
            let guard = data.take(packet_len);
            let cmd = HashedCommand::new(guard, hash, flag, sentonly, req.operation());
            process.process(cmd);
        }
        Ok(())
    }
    #[inline(always)]
    fn operation<C: AsRef<Command>>(&self, cmd: C) -> Operation {
        // 第0个字节就是operation
        (cmd.as_ref().flag() as u8 as usize).into()
    }
    #[inline(always)]
    fn ok(&self, cmd: &Command) -> bool {
        let flag: Flag = cmd.flag().into();
        flag.status_ok()
    }
    #[inline(always)]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Option<Command> {
        let len = data.len();
        if len >= HEADER_LEN {
            let r = data.slice();
            let pl = r.packet_len();
            if len >= pl {
                let mut flag = Flag::from(r.op());
                flag.set_status_ok(r.status_ok());
                return Some(Command::new(0, data.take(pl)));
            }
        }
        None
    }
    fn write_response<W: crate::ResponseWriter>(
        &self,
        req: &HashedCommand,
        resp: &Command,
        w: &W,
    ) -> Result<()> {
        todo!();
    }
}
