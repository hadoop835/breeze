// 定期更新discovery.
use super::{Discover, ServiceId, TopologyWrite};
use crossbeam_channel::Receiver;
use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::interval;

use std::collections::HashMap;

pub fn start_watch_discovery<D, T>(snapshot: &str, discovery: D, rx: Receiver<T>, tick: Duration)
where
    T: Send + TopologyWrite + ServiceId + 'static,
    D: Send + Sync + Discover + Unpin + 'static,
{
    let snapshot = snapshot.to_string();
    tokio::spawn(async move {
        log::info!("discovery watch task started");
        let mut refresher = Refresher {
            snapshot: snapshot,
            discovery: discovery,
            rx: rx,
            tick: tick,
        };
        refresher.watch().await;
        log::info!("discovery watch task complete");
    });
}
unsafe impl<D, T> Send for Refresher<D, T> {}
unsafe impl<D, T> Sync for Refresher<D, T> {}

struct Refresher<D, T> {
    discovery: D,
    snapshot: String,
    tick: Duration,
    rx: Receiver<T>,
}

impl<D, T> Refresher<D, T>
where
    D: Discover + Send + Unpin,
    T: Send + TopologyWrite + ServiceId + 'static,
{
    async fn watch(&mut self) {
        // 降低tick的频率，便于快速从chann中接收新的服务。
        let mut tick = interval(Duration::from_secs(1));
        let mut services = HashMap::new();
        let mut sigs = HashMap::new();
        let mut last = Instant::now();
        loop {
            while let Ok(mut t) = self.rx.try_recv() {
                if services.contains_key(t.name()) {
                    log::error!("service duplicatedly registered:{}", t.name());
                } else {
                    log::info!("service {} path:{} registered ", t.name(), t.path());
                    if let Some((sig, _cfg)) = self.init(&mut t).await {
                        sigs.insert(t.name().to_string(), sig);
                    }
                    services.insert(t.name().to_string(), t);
                }
            }
            if last.elapsed() >= self.tick {
                self.check_once(&mut services, &mut sigs).await;
                last = Instant::now();
            }
            tick.tick().await;
        }
    }
    // 从rx里面获取所有已注册的服务列表
    // 先从cache中取
    // 其次从remote获取
    async fn check_once(
        &mut self,
        services: &mut HashMap<String, T>,
        sigs: &mut HashMap<String, String>,
    ) {
        let mut cache: HashMap<String, (String, String)> = HashMap::with_capacity(services.len());
        for (name, t) in services.iter_mut() {
            let path = t.path().to_string();
            let empty = String::new();
            let sig = sigs.get(name).unwrap_or(&empty);
            // 在某些场景下，同一个name被多个path共用。所以如果sig没有变更，则不需要额外处理更新。
            if let Some((path_sig, cfg)) = cache.get(&path) {
                if path_sig == sig {
                    continue;
                }
                if cfg.len() > 0 {
                    t.update(name, &cfg);
                    continue;
                }
            }

            if let Some((sig, cfg)) = self.load_from_discovery(&path, sig).await {
                sigs.insert(name.to_string(), sig.to_string());
                t.update(name, &cfg);
                cache.insert(path, (sig, cfg));
            }
        }
    }
    // 先从snapshot加载，再从远程加载
    async fn init(&self, t: &mut T) -> Option<(String, String)> {
        let path = t.path().to_string();
        let name = t.name().to_string();
        // 用path查找，用name更新。
        if let Ok((sig, cfg)) = self.try_load_from_snapshot(&path).await {
            t.update(&name, &cfg);
            Some((sig, cfg))
        } else {
            if let Some((sig, cfg)) = self.load_from_discovery(&path, "").await {
                t.update(&name, &cfg);
                Some((sig, cfg))
            } else {
                None
            }
        }
    }
    async fn try_load_from_snapshot(&self, name: &str) -> Result<(String, String)> {
        let mut contents = Vec::with_capacity(8 * 1024);
        File::open(&self._path(name))
            .await?
            .read_to_end(&mut contents)
            .await?;
        let mut contents = String::from_utf8(contents)
            .map_err(|_e| Error::new(ErrorKind::Other, "not a valid utfi file"))?;
        // 内容的第一行是签名，第二行是往后是配置
        let idx = contents.find('\n').unwrap_or(0);
        let cfg = contents.split_off(idx);
        let sig = contents;
        log::info!("{} snapshot loaded:sig:{} cfg:{}", name, sig, cfg.len());
        Ok((sig, cfg))
    }
    async fn load_from_discovery(&self, path: &str, sig: &str) -> Option<(String, String)> {
        use super::Config;
        match self.discovery.get_service::<String>(path, &sig).await {
            Err(e) => {
                log::warn!("load service topology failed. path:{}, e:{:?}", path, e);
            }
            Ok(c) => match c {
                Config::NotChanged => {}
                Config::NotFound => {
                    log::info!("{} not found in discovery", path);
                }
                Config::Config(sig, cfg) => {
                    self.dump_to_snapshot(path, &sig, &cfg).await;
                    return Some((sig, cfg));
                }
            },
        }
        None
    }
    fn _path(&self, name: &str) -> PathBuf {
        let base = name.replace("/", "+");
        let mut pb = PathBuf::new();
        pb.push(&self.snapshot);
        pb.push(base);
        pb
    }
    async fn dump_to_snapshot(&self, name: &str, sig: &str, cfg: &str) {
        log::info!("dump {} to snapshot. sig:{} cfg:{}", name, sig, cfg.len());
        match self.try_dump_to_snapshot(name, sig, cfg).await {
            Ok(_) => {}
            Err(e) => {
                log::warn!(
                    "failed to dump (name:{} sig:{}) cfg len:{} to snapshot err:{:?}",
                    name,
                    sig,
                    cfg.len(),
                    e
                )
            }
        }
    }
    async fn try_dump_to_snapshot(&self, name: &str, sig: &str, cfg: &str) -> Result<()> {
        let path = self._path(name);
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let mut file = File::create(path).await?;
        file.write_all(sig.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.write_all(cfg.as_bytes()).await?;
        Ok(())
    }
}
