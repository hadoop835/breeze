extern crate json;
use async_recursion::async_recursion;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use url::Url;
#[derive(Clone)]
pub struct Vintage {
    client: Client,
    base_url: Url,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RNode {
    name: String,
    data: String,
    index: String,
    children: Vec<RChildren>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct RChildren {
    name: String,
    data: String,
}
impl RChildren {
    fn rname(self) -> (String, String) {
        (self.name, self.data)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct RResponse {
    message: String,
    node: RNode,
}
impl RResponse {
    fn rd_into(self) -> (String, String, String) {
        (self.node.name, self.node.index, self.node.data)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    message: String,
    node: Node,
}
#[derive(Serialize, Deserialize, Debug)]
struct Node {
    index: String,
    name: String,
    data: String,
}

impl Response {
    fn into(self) -> (String, String) {
        (self.node.index, self.node.data)
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Redisyaml {
    basic: HashMap<String, String>,
    //backends: Vec<Vec<String>>,
    backends: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Basic {
    access_mod: String,
    hash: String,
    distribution: String,
    listen: String,
    resource_type: String,
}

impl Vintage {
    pub fn from_url(url: Url) -> Self {
        Self {
            base_url: url,
            client: Client::new(),
        }
    }

    async fn lookup<C>(&self, path: &str, index: &str) -> std::io::Result<Config<C>>
    where
        C: From<String>,
    {
        // 设置config的path
        let mut gurl = self.base_url.clone();
        gurl.set_path(path);
        log::debug!("lookup: path:{} index:{}", path, index);
        let resp = self
            .client
            .get(gurl)
            .query(&[("index", index)])
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        match resp.status().as_u16() {
            // not modified
            304 => Ok(Config::NotChanged),
            404 => Ok(Config::NotFound),
            200 => {
                let resp: Response = resp
                    .json()
                    .await
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                if resp.message != "ok" {
                    Err(Error::new(ErrorKind::Other, resp.message))
                } else {
                    let (t_index, data) = resp.into();
                    if t_index == index {
                        Ok(Config::NotChanged)
                    } else {
                        log::info!("{} from {} to {} len:{}", path, index, t_index, data.len());
                        Ok(Config::Config(t_index, C::from(data)))
                    }
                }
            }
            status => {
                let msg = format!("{} not a valid vintage status.", status);
                Err(Error::new(ErrorKind::Other, msg))
            }
        }
    }

    #[async_recursion]
    async fn recursion(
        &self,
        mut url: String,
        index: String,
        mut map: HashMap<String, String>,
    ) -> std::io::Result<HashMap<String, String>> {
        let mut end_url = Url::parse(&url).unwrap();
        let http_resp = self
            .client
            .get(end_url)
            .query(&[("children", "true")])
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let uresp: RResponse = http_resp
            .json()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let (mut t_name, index, mut t_data) = uresp.clone().rd_into();
        let mut vec: Vec<String> = vec![];
        for child in uresp.node.children.iter() {
            t_name = child.name.clone();
            t_data = child.data.clone();
            let mut end_url = url.clone();
            end_url.push_str("/");
            end_url.push_str(&t_name);
            if t_data.is_empty() {
                map = self
                    .recursion(end_url.clone(), index.clone(), map.clone())
                    .await?;
            } else {
                map.insert(end_url.clone(), t_data.clone());
            }
        }
        Ok(map)
    }

    async fn rdlookup<C>(&self, uname: String, index: String) -> std::io::Result<Config<C>>
    where
        C: From<String>,
    {
        let mut f_url = self.base_url.clone().to_string();
        f_url.push_str(&uname);
        let aurl = Url::parse(&f_url).unwrap();

        let http_resp = self
            .client
            .get(aurl)
            .query(&[("children", "true"), ("index", &index)])
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        match http_resp.status().as_u16() {
            // not modified
            304 => Ok(Config::NotChanged),
            404 => Ok(Config::NotFound),
            200 => {
                let uresp: RResponse = http_resp
                    .json()
                    .await
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                let (t_name, t_index, mut data) = uresp.clone().rd_into();
                if uresp.message != "ok" {
                    Err(Error::new(ErrorKind::Other, uresp.message))
                } else {
                    if t_index == index {
                        Ok(Config::NotChanged)
                    } else {
                        let mut map = HashMap::new();
                        let mut basicdata = HashMap::new();
                        let mut backendsdata = Vec::new();
                        let mut mapbackends = HashMap::new();
                        map = self.recursion(f_url, t_index.clone(), map).await?;
                        for (key, val) in &map {
                            if key.contains("shards") {
                                mapbackends.insert(key.to_string(), val.to_string());
                                let mut vec: Vec<String> =
                                    mapbackends.clone().into_values().collect();
                                vec.sort();
                                let mut sort_domain = String::new();
                                let mut rest = Vec::new();
                                for i in 0..vec.len() {
                                    let backend_len = vec[i].rfind(':').unwrap_or(0);
                                    let name = vec[i].clone().split_off(backend_len + 1);
                                    for j in (i + 1)..vec.len() {
                                        if vec[j].contains(&name) {
                                            sort_domain.push_str(&vec[i].clone());
                                            sort_domain.push_str(",");
                                            sort_domain.push_str(&vec[j].clone());
                                            if sort_domain.contains(",") {
                                                rest.push(sort_domain.clone());
                                                sort_domain.clear();
                                            }
                                            break;
                                        }
                                    }
                                }
                                backendsdata = rest;
                            } else {
                                let basic_len = key.rfind('/').unwrap_or(0);
                                let name = key.clone().split_off(basic_len + 1);
                                basicdata.insert(name, val.to_string());
                            }
                        }
                        println!("basbdnaxhs----{:?}", backendsdata);
                        let yaml: Redisyaml = Redisyaml {
                            basic: basicdata,
                            backends: backendsdata,
                        };
                        match serde_yaml::to_string::<Redisyaml>(&yaml.clone()) {
                            Err(e) => {
                                println!("parse to yaml cfg failed:{:?}", e);
                                return Err(Error::new(ErrorKind::AddrNotAvailable, e));
                            }
                            Ok(to_yaml) => {
                                data = to_yaml;
                            }
                        };

                        println!("最终的sdata\n{}", data);
                        log::info!(" from {} to {} len:{}", index, t_index, data.len());
                        //Ok(Config::NotChanged)
                        Ok(Config::Config(t_index, C::from(data)))
                    }
                }
            }
            status => {
                let msg = format!("{} not a valid vintage status.", status);
                Err(Error::new(ErrorKind::Other, msg))
            }
        }
    }
}

use super::Config;
use async_trait::async_trait;
//use protocol::Resource;
#[async_trait]
impl super::Discover for Vintage {
    #[inline]
    async fn get_service<C>(
        &self,
        name: &str,
        sig: &str,
        kindof_database: &str,
    ) -> std::io::Result<Config<C>>
    where
        C: Unpin + Send + From<String>,
    {
        match kindof_database {
            "mc" => self.lookup(name, sig).await,
            "redis" => self.rdlookup(name.to_owned(), sig.to_owned()).await,
            _ => {
                let msg = format!("not a valid vintage database");
                return Err(Error::new(ErrorKind::Other, msg));
            }
        }
    }
}
