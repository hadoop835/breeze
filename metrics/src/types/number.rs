use super::{base::Adder, IncrTo, ItemData0};
use crate::ItemWriter as Writer;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Count;
impl super::Snapshot for Count {
    // 只计数。
    #[inline]
    fn snapshot<W: Writer>(&self, path: &str, key: &str, data: &ItemData0, w: &mut W, _secs: f64) {
        let cur = data.d0.get();
        if cur > 0 {
            w.write(path, key, "num", cur);
        }
    }
    fn merge(&self, global: &ItemData0, cache: &ItemData0) {
        global.d0.incr_by(cache.d0.take());
    }
}

// 对于计数类的，只用第一个来计数
impl IncrTo for i64 {
    #[inline]
    fn incr_to(&self, data: &ItemData0) {
        data.d0.incr_by(*self);
    }
}
