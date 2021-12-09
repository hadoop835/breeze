#[macro_use]
extern crate lazy_static;

mod id;
pub use id::*;

mod ip;
pub use ip::*;

mod sender;
use sender::Sender;

use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc::channel;

mod seg;
pub use seg::*;

pub fn start_metric_sender(addr: &str) {
    start_register_metrics();
    let send = Sender::new(addr);
    send.start_sending();
}

pub use types::Status;
mod packet;

mod macros;
mod types;
use types::*;

mod item;
use item::*;
mod kv;
use kv::*;
