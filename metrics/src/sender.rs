use super::packet::PacketBuffer;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use tokio::time::{interval, Interval};

pub(crate) struct Sender {
    tick: Interval,
    packet: PacketBuffer,
    last: Instant,
}

impl Sender {
    pub(crate) fn new(addr: &str) -> Self {
        let mut tick = interval(Duration::from_secs(10));

        Self {
            packet: PacketBuffer::new(addr.to_string()),
            last: Instant::now(),
            tick: interval(Duration::from_secs(10)),
        }
    }
    pub fn start_sending(self) {
        tokio::spawn(async move {
            log::info!("metric-send: task started:{}", &self.packet.addr);
            self.await;
        });
    }
}

impl Future for Sender {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = &mut *self;
        ready!(me.packet.poll_flush(cx));
        loop {
            ready!(me.tick.poll_tick(cx));
            // 判断是否可以flush
            let elapsed = me.last.elapsed();
            let metrics = crate::get_metrics();
            metrics.write(&mut me.packet);
            ready!(me.packet.poll_flush(cx));
        }
    }
}
unsafe impl Send for Sender {}
unsafe impl Sync for Sender {}
