mod meta;
pub mod props;
mod protocol;

use std::{collections::HashSet, time::Duration};

use trust_dns_resolver::{AsyncResolver, TokioConnection, TokioConnectionProvider, TokioHandle};
type Resolver = AsyncResolver<TokioConnection, TokioConnectionProvider>;

use rocket::{Build, Rocket};

#[macro_use]
extern crate rocket;

#[macro_use]
extern crate lazy_static;

use metrics::Path;

const API_PATH: &str = "api";

// 整合所有routers
pub fn routes() -> Rocket<Build> {
    let mut rocket = rocket::build();

    // 元数据相关routes
    rocket = meta::routes(rocket);

    // 各种协议 cmd相关routes
    protocol::routes(rocket)
}

// 定期刷新白名单域名
pub async fn start_whitelist_refresh(host: String) {
    let resolver: Resolver =
        AsyncResolver::from_system_conf(TokioHandle).expect("crate api dns resolver");

    // 每10分钟刷新一次
    let mut tick = tokio::time::interval(Duration::from_secs(10 * 60));
    loop {
        tick.tick().await;

        let mut whitelist = HashSet::with_capacity(2);
        match resolver.lookup_ip(host.clone()).await {
            Ok(ips) => {
                for ip in ips.iter() {
                    whitelist.insert(ip.to_string());
                }
            }
            Err(err) => {
                log::warn!("api - parse whitelist host {} failed: {:?}", host, err);
            }
        }
        if whitelist.len() > 0 {
            // 合法域名时，同时将localhost加入，支持本地访问
            whitelist.insert("127.0.0.1".to_string());
            props::update_whitelist(whitelist);
        }
    }
}

// 统计
fn qps_incr(name: &'static str) {
    let mut opts = Path::new(vec![API_PATH]).qps(name);
    opts += 1;
}

// 校验client，当前只检查ip白名单
fn verify_client(client_ip: &String) -> bool {
    if props::is_in_whitelist(client_ip) {
        return true;
    }
    log::info!("api - found illegal user: {}", client_ip);
    false
}
