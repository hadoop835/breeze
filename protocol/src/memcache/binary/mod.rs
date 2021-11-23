mod meta;
use meta::*;

use crate::{Error, Flag, Operation, Result, Stream};

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
        while data.len() >= HEADER_LEN {
            let req = data.slice();
            let packet_len = req.packet_len();
            if req.len() < packet_len {
                break;
            }
            // 把quite get请求，转换成单个的get请求
            if req.quite_get() {
                let idx = PacketPos::Opcode as usize;
                data.update(idx, UN_MULT_GETS_OPS[req.op() as usize]);
            }
            let mut flag = Flag::from_op(req.op());
            if NOREPLY_MAPPING[req.op() as usize] == req.op() {
                flag.sentonly();
            }
            let hash = alg.hash(&req.key());
            let guard = data.take(packet_len);
            let cmd = HashedCommand::new(guard, hash, flag);
            process.process(cmd);
        }
        Ok(())
    }
    #[inline(always)]
    fn operation<C: AsRef<Command>>(&self, cmd: C) -> Operation {
        cmd.as_ref().flag().get_operation()
    }
    #[inline(always)]
    fn ok(&self, cmd: &Command) -> bool {
        cmd.flag().is_status_ok()
    }
    #[inline(always)]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Option<Command> {
        let len = data.len();
        if len >= HEADER_LEN {
            let r = data.slice();
            let pl = r.packet_len();
            if len >= pl {
                let mut flag = Flag::from_op(r.op());
                if r.status_ok() {
                    flag.status_ok();
                }
                return Some(Command::new(flag, data.take(pl)));
            }
        }
        None
    }
    #[inline(always)]
    fn write_response<W: crate::ResponseWriter>(
        &self,
        _req: &HashedCommand,
        resp: &Command,
        w: &mut W,
    ) -> Result<()> {
        let len = resp.len();
        let mut oft = 0;
        while oft < len {
            let data = resp.read(oft);
            w.write(data)?;
            oft += data.len();
        }
        Ok(())
    }
}
