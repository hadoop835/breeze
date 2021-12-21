use crate::{Metric, MetricType};
#[derive(Debug, Hash, PartialEq, Eq, Default)]
pub(crate) struct Id {
    pub(crate) path: String,
    pub(crate) key: &'static str,
    pub(crate) t: MetricType,
}
impl Id {
    #[inline(always)]
    pub(crate) fn valid(&self) -> bool {
        self.path.len() > 0 && !self.t.is_empty()
    }
}

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
        let id = Id { path: s, key, t };
        crate::register_metric(id)
    }
}

use std::collections::HashMap;
use std::sync::Arc;
#[derive(Default)]
struct IdSequence {
    seq: usize,
    indice: HashMap<Arc<Id>, usize>,
}

impl IdSequence {
    fn get_idx(&self, id: &Arc<Id>) -> Option<usize> {
        self.indice.get(id).map(|idx| *idx)
    }
    fn register_name(&mut self, name: &Arc<Id>) -> usize {
        match self.indice.get(name) {
            Some(seq) => *seq,
            None => {
                let seq = self.seq;
                log::debug!("metric name registered. index:{} name:{}", seq, name.path);
                self.indice.insert(name.clone(), seq);
                self.seq += 1;
                seq
            }
        }
    }
}

use std::sync::RwLock;
lazy_static! {
    static ref ID_SEQ: RwLock<IdSequence> = RwLock::new(Default::default());
}

// 返回对应的idx，以及该size是否已经注册过。
pub(crate) fn register_name(id: &Arc<Id>) -> (usize, bool) {
    let seqs = ID_SEQ.read().unwrap();
    if let Some(idx) = seqs.get_idx(id) {
        return (idx, true);
    }
    // 必须显示释放锁。
    drop(seqs);
    // 加写锁
    let mut seqs = ID_SEQ.write().unwrap();
    // double check
    if let Some(idx) = seqs.get_idx(id) {
        return (idx, true);
    }
    (seqs.register_name(id), false)
}
