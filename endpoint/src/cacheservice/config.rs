use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Default, Hash)]
pub struct Namespace {
    #[serde(default)]
    pub hash: String, // eg: bkdr
    #[serde(default)]
    pub distribution: String, //eg: ketama
    #[serde(default)]
    pub hash_tag: String, //eg: user
    //pub timeout: i32,         // unit: mills
    //pub exptime: i64,
    #[serde(default)]
    pub master: Vec<String>,
    #[serde(default)]
    pub master_l1: Vec<Vec<String>>,
    #[serde(default)]
    pub slave: Vec<String>,
    #[serde(default)]
    pub slave_l1: Vec<Vec<String>>,
}

impl Namespace {
    pub(crate) fn parse<F: FnMut(u64, Self)>(sig: u64, group_cfg: &str, namespace: &str, mut f: F) {
        log::debug!("group_cfg:{:?}", group_cfg);
        let mut cfg = Config::new(group_cfg.as_bytes());
        match cfg.find(|(k, _v)| k == &namespace.as_bytes()) {
            Some((_k, v)) => {
                use std::collections::hash_map::DefaultHasher;
                let mut h = DefaultHasher::default();
                use std::hash::{Hash, Hasher};
                v.hash(&mut h);
                let new = h.finish();
                if new != sig {
                    match serde_yaml::from_str::<Namespace>(unsafe {
                        std::str::from_utf8_unchecked(v)
                    }) {
                        Ok(ns) => {
                            f(new, ns);
                        }
                        Err(e) => {
                            log::warn!("parse namespace error. {} msg:{:?}", namespace, e);
                        }
                    }
                }
            }
            None => {
                log::info!("'{}' namespace not found", namespace);
            }
        }
    }
}

impl Namespace {
    // 可写的实例。第一组一定是master. 包含f： master, master-l1, slave, slave-l1
    //pub fn writers(&self) -> Vec<(LayerRole, Vec<String>)> {
    //    let mut w = Vec::with_capacity(8);
    //    if self.master.len() > 0 {
    //        w.push((LayerRole::Master, self.master.clone()));
    //        // w.extend(self.master_l1.clone());
    //        for ml1 in self.master_l1.iter() {
    //            w.push((LayerRole::MasterL1, ml1.clone()));
    //        }
    //        w.push((LayerRole::Slave, self.slave.clone()));
    //        // w.extend(self.slave_l1.clone());
    //        for sl1 in self.slave_l1.iter() {
    //            w.push((LayerRole::SlaveL1, sl1.clone()));
    //        }
    //    }
    //    w
    //}
    //pub fn uniq_all(&self) -> Vec<(LayerRole, Vec<String>)> {
    //    let mut all = vec![(LayerRole::Master, self.master.clone())];
    //    // all.extend(self.master_l1.clone());
    //    for ml1 in self.master_l1.iter() {
    //        all.push((LayerRole::MasterL1, ml1.clone()));
    //    }
    //    // all.extend(self.slave_l1.clone());
    //    for sl1 in self.slave_l1.iter() {
    //        all.push((LayerRole::SlaveL1, sl1.clone()));
    //    }
    //    all.push((LayerRole::Slave, self.slave.clone()));
    //    all
    //}
}

struct Config<'a> {
    oft: usize,
    data: &'a [u8],
}

impl<'a> Iterator for Config<'a> {
    type Item = (&'a [u8], &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(key) = self.next_key_line() {
            let val_start = self.oft;
            let val_end = if let Some(val) = self.next_key_line() {
                let start = val.0;
                self.oft = start;
                start
            } else {
                self.data.len()
            };
            let k = &self.data[key.0..key.1];
            let v = &self.data[val_start..val_end];
            Some((k, v))
        } else {
            None
        }
    }
}

impl<'a> Config<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { oft: 0, data }
    }
    // 指针指到下一行的开始
    fn skip_line(&mut self) {
        while self.oft < self.data.len() {
            let c = self.data[self.oft];
            self.oft += 1;
            if c == b'\n' {
                break;
            }
        }
    }
    fn next_key_line(&mut self) -> Option<(usize, usize)> {
        while self.oft < self.data.len() {
            let c = self.data[self.oft];
            if c == b' ' || c == b'#' {
                self.skip_line();
                continue;
            }
            let start = self.oft;
            let mut end = start;
            self.skip_line();
            // 找到':'
            for i in start..self.oft {
                if self.data[i] == b':' {
                    end = i;
                    break;
                }
            }
            if end > start {
                return Some((start, end));
            }
        }
        None
    }
}
