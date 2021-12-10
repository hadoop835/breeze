mod meta;
use meta::*;
mod packet;
use packet::*;

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
            let mut flag = Flag::from_op(req.op(), req.operation());
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
    //#[inline(always)]
    //fn operation<C: AsRef<Command>>(&self, cmd: C) -> Operation {
    //    cmd.as_ref().flag().get_operation()
    //}
    //#[inline(always)]
    //fn ok(&self, cmd: &Command) -> bool {
    //    cmd.flag().is_status_ok()
    //}
    #[inline(always)]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        debug_assert!(data.len() > 0);
        if data.at(PacketPos::Magic as usize) != RESPONSE_MAGIC {
            return Err(Error::ProtocolNotValid);
        }
        let len = data.len();
        if len >= HEADER_LEN {
            let r = data.slice();
            let pl = r.packet_len();
            if len >= pl {
                let mut flag = Flag::from_op(r.op(), r.operation());
                if r.status_ok() {
                    flag.set_status_ok();
                }
                return Ok(Some(Command::new(flag, data.take(pl))));
            }
        }
        Ok(None)
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
    #[inline]
    fn convert_to_writeback_request(
        &self,
        req: &HashedCommand,
        resp: &Command,
        exp_sec: u32,
    ) -> HashedCommand {
        // 轮询response的cmds，构建回写request
        // 只为status为ok的resp构建回种req
        debug_assert!(resp.ok());
        let rsp_cmd = resp.data();
        let r_data = req.data();
        let key_len = r_data.key_len();
        // 4 为expire flag的长度。
        // 先用rsp的精确长度预分配，避免频繁分配内存
        let req_cmd_len = rsp_cmd.len() + 4 + key_len as usize;
        let mut req_cmd: Vec<u8> = Vec::with_capacity(req_cmd_len);
        use ds::Buffer;

        /*============= 构建request header =============*/
        req_cmd.push(Magic::Request as u8); // magic: [0]
        req_cmd.push(Opcode::SetQ as u8); // opcode: [1]
        req_cmd.write_u16(key_len); // key len: [2,3]
        let extra_len = rsp_cmd.extra_len() + 4 as u8; // get response中的extra 应该是4字节，作为set的 flag，另外4字节是set的expire
        debug_assert!(extra_len == 8);
        req_cmd.push(extra_len); // extra len: [4]
        req_cmd.push(0 as u8); // data type，全部为0: [5]
        req_cmd.write_u16(0 as u16); // vbucket id, 回写全部为0,pos [6,7]
        let total_body_len = extra_len as u32 + key_len as u32 + rsp_cmd.value_len();
        req_cmd.write_u32(total_body_len); // total body len [8-11]
        req_cmd.write_u32(0 as u32); // opaque: [12, 15]
        req_cmd.write_u64(0 as u64); // cas: [16, 23]

        /*============= 构建request body =============*/
        rsp_cmd.extra_or_flag().copy_to_vec(&mut req_cmd); // extra之flag: [24, 27]
        req_cmd.write_u32(exp_sec); // extra之expiration：[28,31]
        r_data.key().copy_to_vec(&mut req_cmd);
        rsp_cmd.value().copy_to_vec(&mut req_cmd);

        if req_cmd.capacity() > req_cmd_len {
            log::info!("capacity bigger:{}/{}", req_cmd_len, req_cmd.capacity());
        }

        let hash = req.hash();
        let mut flag = Flag::from_op(r_data.op(), r_data.operation());
        flag.sentonly();
        let guard = ds::MemGuard::from_vec(req_cmd);
        HashedCommand::new(guard, hash, flag)
    }
}
