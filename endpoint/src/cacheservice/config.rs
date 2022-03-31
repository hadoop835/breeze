use serde::{Deserialize, Serialize};
use sharding::hash;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug, Default, Hash)]
pub struct Namespace {
    #[serde(default)]
    pub hash: String, // eg: bkdr
    #[serde(default)]
    pub distribution: String, //eg: ketama
    #[serde(default)]
    pub hash_tag: String, //eg: user
    //pub timeout: i32,         // unit: mills
    pub exptime: i64,
    #[serde(default)]
    pub master: Vec<String>,
    #[serde(default)]
    pub master_l1: Vec<Vec<String>>,
    #[serde(default)]
    pub slave: Vec<String>,
    #[serde(default)]
    pub slave_l1: Vec<Vec<String>>,

    // set master 失败后，是否更新其他各层
    #[serde(default)]
    pub force_write_all: bool,

    // set/cas/add/delete等更新操作，是否更新slave L1
    pub update_slave_l1: bool,

    #[serde(default)]
    pub timeout_ms_master: u32,
    #[serde(default)]
    pub timeout_ms_slave: u32,
}

impl Namespace {
    pub(crate) fn try_from(cfg: &str, namespace: &str) -> Option<Self> {
        log::debug!("namespace:{} cfg:{} updating", namespace, cfg);
        match serde_yaml::from_str::<Namespace>(cfg) {
            Err(e) => {
                log::warn!("parse namespace error. {} msg:{:?}", namespace, e);
                None
            }
            Ok(mut ns) => {
                if ns.master.len() == 0 {
                    log::info!("cache service master empty. namespace:{}", namespace);
                    None
                } else {
                    // 对于mc，crc32实际是crc32-short，这里需要做一次转换
                    if ns.hash.eq("crc32") {
                        ns.hash = format!(
                            "crc32{}{}",
                            hash::HASHER_NAME_DELIMITER,
                            hash::CRC32_EXT_SHORT
                        );
                        log::debug!("change mc crc32 to {}", ns.hash);
                    }

                    // 如果update_slave_l1为false，去掉slave_l1
                    if !ns.update_slave_l1 {
                        ns.slave_l1 = Vec::with_capacity(0);
                    }

                    Some(ns)
                }
            }
        }
    }
    pub(super) fn timeout_master(&self) -> Duration {
        Duration::from_millis(200.max(self.timeout_ms_master as u64))
    }
    pub(super) fn timeout_slave(&self) -> Duration {
        Duration::from_millis(80.max(self.timeout_ms_slave as u64))
    }
}

impl Namespace {}

pub(crate) struct Config<'a> {
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
    pub(crate) fn new(data: &'a [u8]) -> Self {
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
