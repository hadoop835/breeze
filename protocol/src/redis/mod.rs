#[derive(Clone, Default)]
pub struct Redis;
use crate::{
    Command, Commander, Error, Flag, HashedCommand, Protocol, RequestProcessor, Result, Stream,
};
use sharding::hash::Hash;
impl Protocol for Redis {
    #[inline(always)]
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        todo!();
    }
    #[inline(always)]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        todo!();
    }
    #[inline(always)]
    fn write_response<C: Commander, W: crate::ResponseWriter>(
        &self,
        ctx: &mut C,
        w: &mut W,
    ) -> Result<()> {
        todo!();
    }
    #[inline(always)]
    fn write_no_response<W: crate::ResponseWriter>(
        &self,
        _req: &HashedCommand,
        _w: &mut W,
    ) -> Result<()> {
        todo!();
    }
    // 构建回写请求。
    // 返回None: 说明req复用，build in place
    // 返回新的request
    #[inline(always)]
    fn build_writeback_request<C: Commander>(&self, _ctx: &mut C, _: u32) -> Option<HashedCommand> {
        todo!("not implement");
    }
}
