use super::SocketAddr;
use super::Stream;

use std::io::Error;
use std::io::ErrorKind;
use std::str::FromStr;

use tokio::io;
use tokio::net::TcpSocket;
use tokio::net::{TcpListener, UnixListener};

pub enum Listener {
    Unix(UnixListener),
    Tcp(TcpListener),
}

impl Listener {
    pub async fn bind(protocol: &str, addr: &str) -> std::io::Result<Self> {
        match protocol {
            "unix" => Ok(Listener::Unix(UnixListener::bind(addr)?)),
            "tcp" => Ok(Listener::Tcp(Listener::listen_reuseport(addr).await?)),
            _ => Err(Error::new(ErrorKind::InvalidInput, addr)),
        }
    }

    pub async fn listen_reuseport(addr: &str) -> io::Result<TcpListener> {
        let addr = std::net::SocketAddr::from_str(addr).unwrap();
        let socket = match addr {
            std::net::SocketAddr::V4(_) => TcpSocket::new_v4(),
            std::net::SocketAddr::V6(_) => TcpSocket::new_v6(),
        }?;
        socket.set_reuseport(true)?;
        socket.bind(addr)?;
        Ok(socket.listen(1024)?)
    }

    pub async fn accept(&self) -> std::io::Result<(Stream, SocketAddr)> {
        match self {
            Listener::Unix(l) => {
                let (stream, addr) = l.accept().await?;
                Ok((stream.into(), SocketAddr::Unix(addr)))
            }
            Listener::Tcp(l) => {
                let (stream, addr) = l.accept().await?;
                //stream.set_nodelay(true)?;
                Ok((stream.into(), SocketAddr::Tcp(addr)))
            }
        }
    }
}
