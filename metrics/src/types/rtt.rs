use std::time::Duration;

use crate::{Id, ItemWriter, NumberInner};
pub const MAX: Duration = Duration::from_millis(30);
const SLOW_US: i64 = Duration::from_millis(100).as_micros() as i64;
const MAX_US: i64 = MAX.as_micros() as i64;
pub struct Rtt {
    count: NumberInner,
    total_us: NumberInner,
    slow: NumberInner,
    max: NumberInner,
}

impl Rtt {
    #[inline]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        // qps
        let count = self.count.take();
        if count > 0 {
            w.write(&id.path, id.key, "qps", count as f64 / secs);
            // avg_us
            let total_us = self.total_us.take() as f64;
            // 按微秒取整
            let avg = (total_us / count as f64) as isize as f64;
            w.write(&id.path, id.key, "avg_us", avg);

            w.write(&id.path, id.key, "total_num", count as f64);
            w.write(&id.path, id.key, "total_us", total_us);
            // slow qps
            let slow = self.slow.take();
            if slow > 0 {
                w.write(&id.path, id.key, "qps_itvl100ms", slow as f64 / secs);
            }
            let max = self.max.zero();
            if max > 0 {
                w.write(&id.path, id.key, "max_us", max as f64);
            }
        }
    }

    #[inline]
    pub(crate) fn incr(&self, d: Duration) {
        self.count.incr(1);
        let us = d.as_micros() as i64;
        self.total_us.incr(us);
        if us >= SLOW_US {
            self.slow.incr(1);
        }
        if us >= MAX_US {
            self.max.max(us);
        }
    }
}
