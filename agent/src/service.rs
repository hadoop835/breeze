use net::listener::Listener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;

use context::Quadruple;
use crossbeam_channel::Sender;
use discovery::TopologyWriteGuard;
use metrics::MetricName;
use protocol::callback::Callback;
use protocol::{Parser, Result};
use stream::pipeline::copy_bidirectional;
use stream::Builder;

use stream::Request;
type Endpoint = Arc<stream::Backend<Request>>;
type Topology = endpoint::Topology<Builder<Parser, Request>, Endpoint, Request, Parser>;
// 一直侦听，直到成功侦听或者取消侦听（当前尚未支持取消侦听）
// 1. 尝试侦听之前，先确保服务配置信息已经更新完成
pub(super) async fn process_one(
    quard: &Quadruple,
    discovery: Sender<TopologyWriteGuard<Topology>>,
    session_id: Arc<AtomicUsize>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let p = Parser::try_from(&quard.protocol())?;
    let top = endpoint::Topology::try_from(p.clone(), quard.endpoint())?;
    let (tx, rx) = discovery::topology(top, &quard.service());
    // 注册，定期更新配置
    discovery.send(tx)?;

    // 等待初始化完成
    let mut tries = 0usize;
    while !rx.inited() {
        if tries >= 2 {
            log::info!("waiting inited. {} ", quard);
        }
        tokio::time::sleep(Duration::from_secs(1 << (tries.min(10)))).await;
        tries += 1;
    }
    log::info!("service inited. {} ", quard);
    let switcher = ds::Switcher::from(true);
    let top = Arc::new(RefreshTopology::new(rx, switcher.clone()));
    let receiver = top.as_ref() as *const RefreshTopology<Topology> as usize;
    let cb = RefreshTopology::<Topology>::static_send;
    let cb = Callback::new(receiver, cb);

    // 服务注册完成，侦听端口直到成功。
    while let Err(e) = _process_one(quard, p.clone(), top.clone(), session_id.clone(), &cb).await {
        log::warn!("service process failed. {}, err:{:?}", quard, e);
        tokio::time::sleep(Duration::from_secs(6)).await;
    }
    switcher.off();

    // TODO 延迟一秒，释放top内存。
    // 因为回调，有可能在连接释放的时候，还在引用top。
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(())
}

use endpoint::RefreshTopology;
async fn _process_one(
    quard: &Quadruple,
    p: Parser,
    top: Arc<RefreshTopology<Topology>>,
    session_id: Arc<AtomicUsize>,
    cb: &Callback,
) -> Result<()> {
    let l = Listener::bind(&quard.family(), &quard.address()).await?;

    let mid = metrics::register!(quard.protocol(), &quard.biz());
    let metric_id = mid.id();
    log::info!("service started. {}", quard);

    loop {
        let top = top.clone();
        // 等待初始化成功
        let (client, _addr) = l.accept().await?;
        let p = p.clone();
        let session_id = session_id.fetch_add(1, Ordering::AcqRel);
        let cb = cb.clone();
        spawn(async move {
            metrics::qps("conn", 1, metric_id);
            metrics::count("conn", 1, metric_id);
            if let Err(e) = copy_bidirectional(top, client, p, session_id, metric_id, cb).await {
                log::debug!("{} disconnected. {:?} ", metric_id.name(), e);
            }
            metrics::count("conn", -1, metric_id);
        });
    }
}

use tokio::net::TcpListener;
// 监控一个端口，主要用于进程监控
pub(super) async fn listener_for_supervisor(port: u16) -> Result<TcpListener> {
    let addr = format!("127.0.0.1:{}", port);
    let l = TcpListener::bind(&addr).await?;
    Ok(l)
}
