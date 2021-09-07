use context::Context;
use discovery::{Discovery, ServiceDiscovery};

use net::listener::Listener;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stream::io::copy_bidirectional;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::time::{interval_at, Instant};

use protocol::Protocols;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = Context::from_os_args();
    ctx.check()?;

    let _l = listener_for_supervisor(ctx.port()).await?;
    elog::init(ctx.log_dir(), &ctx.log_level)?;

    metrics::init(&ctx.metrics_url());
    metrics::init_local_ip(&ctx.metrics_probe);
    let discovery = Arc::from(Discovery::from_url(ctx.discovery()));
    let mut listeners = ctx.listeners();
    let mut tick = interval_at(
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(3),
    );
    let session_id = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(1);

    loop {
        let quards = listeners.scan().await;
        if let Err(e) = quards {
            log::info!("scan listener failed:{:?}", e);
            tick.tick().await;
            continue;
        }
        for quard in quards.unwrap().iter() {
            let quard = quard.clone();
            let quard_name = quard.name();
            let discovery = Arc::clone(&discovery);
            let tx = tx.clone();
            let session_id = session_id.clone();
            spawn(async move {
                let session_id = session_id.clone();
                match process_one_service(&quard, discovery, session_id).await {
                    Ok(_) => {
                        let _ = tx.send(true).await;
                        log::info!("service listener complete address:{}", quard.address())
                    }
                    Err(e) => {
                        let _ = tx.send(false).await;
                        log::warn!("service listener error:{:?} {}", e, quard.address())
                    }
                };
            });
            if let Some(false) = rx.recv().await {
                listeners.add_fail(&quard_name);
            }
        }

        tick.tick().await;
    }
}

async fn process_one_service(
    quard: &context::Quadruple,
    discovery: Arc<discovery::Discovery>,
    session_id: Arc<AtomicUsize>,
) -> Result<()> {
    let parser = Protocols::from(&quard.protocol()).ok_or(Error::new(
        ErrorKind::InvalidData,
        format!("'{}' is not a valid protocol", quard.protocol()),
    ))?;
    let top = endpoint::Topology::from(parser.clone(), quard.endpoint()).ok_or(Error::new(
        ErrorKind::InvalidData,
        format!("'{}' is not a valid endpoint", quard.endpoint()),
    ))?;
    let l = Listener::bind(&quard.family(), &quard.address()).await?;
    log::info!("starting to serve {}", quard);
    let sd = Arc::new(ServiceDiscovery::new(
        discovery,
        quard.service(),
        quard.snapshot(),
        quard.tick(),
        top,
    ));

    let r_type = quard.protocol();
    let biz = quard.biz();
    let metric_id = metrics::register_name(r_type + "." + &metrics::encode_addr(&biz));
    loop {
        let sd = sd.clone();
        let (client, _addr) = l.accept().await?;
        let endpoint = quard.endpoint().to_owned();
        let parser = parser.clone();
        let session_id = session_id.fetch_add(1, Ordering::AcqRel);
        spawn(async move {
            if let Err(e) =
                process_one_connection(client, sd, endpoint, parser, session_id, metric_id).await
            {
                log::warn!("connection disconnected:{:?}", e);
            }
        });
    }
}

async fn process_one_connection(
    client: net::Stream,
    sd: Arc<ServiceDiscovery<endpoint::Topology<Protocols>>>,
    endpoint: String,
    parser: Protocols,
    session_id: usize,
    metric_id: usize,
) -> Result<()> {
    use endpoint::Endpoint;
    let agent = Endpoint::from_discovery(&endpoint, parser.clone(), sd)
        .await?
        .ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("'{}' is not a valid endpoint type", endpoint),
            )
        })?;
    copy_bidirectional(agent, client, parser, session_id, metric_id).await?;
    Ok(())
}

use tokio::net::TcpListener;
// 监控一个端口，主要用于进程监控
async fn listener_for_supervisor(port: u16) -> Result<TcpListener> {
    let addr = format!("127.0.0.1:{}", port);
    let l = TcpListener::bind(&addr).await?;
    Ok(l)
}
