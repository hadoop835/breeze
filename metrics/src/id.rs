use std::sync::atomic::{AtomicU32, Ordering};

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub(crate) enum MetricType {
    Empty = 0u8,
    Qps,
    Status,
    RTT,   // 耗时
    Count, // 计算总的数量，与时间无关。
}

use crate::Metric;

pub struct Path {
    path: Vec<String>,
}
impl Path {
    pub fn new<T: ToString>(names: Vec<T>) -> Self {
        Self {
            path: names.into_iter().map(|s| s.to_string()).collect(),
        }
    }
    #[inline]
    pub fn qps(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Qps)
    }
    #[inline]
    pub fn status(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Status)
    }
    #[inline]
    pub fn rtt(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::RTT)
    }
    #[inline]
    pub fn count(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Count)
    }
    #[inline]
    fn with_type(&self, key: &'static str, t: MetricType) -> Metric {
        let mut s: String = String::with_capacity(256);
        for name in self.path.iter() {
            s += &crate::encode_addr(name.as_ref());
            s.push('.');
        }
        s.pop();
        s.shrink_to_fit();
        Metric::from(s, key, t)
    }
}
