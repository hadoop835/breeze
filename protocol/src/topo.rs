use enum_dispatch::enum_dispatch;

use crate::Command;

#[enum_dispatch]
pub trait Topology<E, Req> {
    fn hasher(&self) -> &str;
    // 返回当前top是否支持on_not_ok。
    // 如果支持，并且处理了on_not_ok，则返回true;
    // 不支持，什么都没做，则返回false
    #[inline(always)]
    fn on_not_ok(&self, _req: Req) -> bool {
        false
    }
    #[inline(always)]
    fn on_ok(&self, _req: Req, _resp: &Command) {}
}
