use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fs;

use crate::{Timeout, TO_MYSQL_M, TO_MYSQL_S};

//时间间隔，闭区间, 可以是2010, 或者2010-2015
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Interval(pub u16, pub u16);

impl<'de> Deserialize<'de> for Interval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(|interval: String| {
            if interval == ARCHIVE_DEFAULT_KEY {
                return Interval(0, 0);
            }
            let mut interval = interval.split("-");
            let start = interval.next().unwrap().parse().unwrap();
            let end = interval.next();
            let end = if end.is_none() {
                start
            } else {
                end.unwrap().parse().unwrap()
            };
            Interval(start, end)
        })
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MysqlNamespace {
    #[serde(default)]
    pub(crate) basic: Basic,
    #[serde(skip)]
    pub(crate) backends_flaten: Vec<String>,
    #[serde(default)]
    pub(crate) backends: HashMap<Interval, Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    listen: String,
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    pub(crate) selector: String,
    #[serde(default)]
    pub(crate) timeout_ms_master: u32,
    #[serde(default)]
    pub(crate) timeout_ms_slave: u32,
    #[serde(default)]
    pub(crate) db_name: String,
    #[serde(default)]
    pub(crate) db_count: u32,
    #[serde(default)]
    pub(crate) strategy: String,
    #[serde(default)]
    pub(crate) password: String,
    #[serde(default)]
    pub(crate) user: String,
}
pub const ARCHIVE_DEFAULT_KEY: &str = "__default__";

impl MysqlNamespace {
    #[inline]
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        match serde_yaml::from_str::<MysqlNamespace>(cfg) {
            Ok(mut ns) => {
                match ns.decrypt_password() {
                    Ok(password) => ns.basic.password = password,
                    Err(e) => {
                        log::warn!("failed to decrypt password, e:{}", e);
                        return None;
                    }
                }
                ns.backends_flaten = ns.backends.iter().fold(Vec::new(), |mut init, b| {
                    init.extend_from_slice(b.1);
                    init
                });
                Some(ns)
            }
            Err(e) => {
                log::info!("failed to parse mysql  e:{} config:{}", e, cfg);
                None
            }
        }
    }

    #[inline]
    fn decrypt_password(&self) -> Result<String, Box<dyn std::error::Error>> {
        let key_pem = fs::read_to_string(&context::get().key_path)?;
        let encrypted_data = general_purpose::STANDARD.decode(self.basic.password.as_bytes())?;
        let decrypted_data = ds::decrypt::decrypt_password(&key_pem, &encrypted_data)?;
        let decrypted_string = String::from_utf8(decrypted_data)?;
        Ok(decrypted_string)
    }
    pub(super) fn timeout_master(&self) -> Timeout {
        let mut to = TO_MYSQL_M;
        if self.basic.timeout_ms_master > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }
    pub(super) fn timeout_slave(&self) -> Timeout {
        let mut to = TO_MYSQL_S;
        if self.basic.timeout_ms_slave > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }
}
