use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Response};
use protocol::{Operation, Protocol, Request};

use futures::ready;

pub struct AsyncLayerGet<L, P> {
    // 当前从哪个layer开始发送请求
    idx: usize,
    layers: Vec<L>,
    // 每一层访问的请求
    request: Request,
    response: Option<Response>,
    parser: P,
    since: Instant, // 上一层请求开始的时间
}

impl<L, P> AsyncLayerGet<L, P>
where
    L: AsyncWriteAll + AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin,
{
    pub fn from_layers(layers: Vec<L>, p: P) -> Self {
        Self {
            idx: 0,
            layers,
            request: Default::default(),
            response: None,
            parser: p,
            since: Instant::now(),
        }
    }
    // 发送请求，将current cmds发送到所有mc，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        // 当前layer的reader发送请求，直到发送成功
        let mut last_err = None;
        while self.idx < self.layers.len() {
            log::debug!("write to {}-th/{}", self.idx + 1, self.layers.len());
            let reader = unsafe { self.layers.get_unchecked_mut(self.idx) };
            match ready!(Pin::new(reader).poll_write(cx, &self.request)) {
                Ok(_) => return Poll::Ready(Ok(())),
                Err(e) => {
                    self.idx += 1;
                    last_err = Some(e);
                }
            }
        }

        self.idx = 0;
        // write req到所有资源失败，reset并返回err
        Poll::Ready(Err(last_err.unwrap_or(Error::new(
            ErrorKind::NotConnected,
            "layer get do write error",
        ))))
    }

    #[inline(always)]
    fn on_response(&mut self, item: Response) {
        let found = item.keys_num();
        // 记录metrics
        let elapse = self.since.elapsed();
        self.since = Instant::now();
        let metric_id = item.rid().metric_id();
        metrics::qps(get_key_hit_name_by_idx(self.idx), found, metric_id);
        metrics::duration(get_name_by_idx(self.idx), elapse, metric_id);

        match self.request.operation() {
            Operation::Gets => {
                match self.response.as_mut() {
                    Some(response) => response.append(item),
                    None => self.response = Some(item),
                };
            }
            _ => {
                self.response = Some(item);
            }
        }
    }
}

impl<L, P> AsyncWriteAll for AsyncLayerGet<L, P>
where
    L: AsyncWriteAll + AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin,
{
    // 请求某一层
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, req: &Request) -> Poll<Result<()>> {
        if self.request.len() == 0 {
            self.request = req.clone();
            self.since = Instant::now();
        }
        return self.do_write(cx);
    }
}

impl<L, P> AsyncReadAll for AsyncLayerGet<L, P>
where
    L: AsyncReadAll + AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin + Protocol,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.layers.len());
        let mut last_err = None;

        while me.idx < me.layers.len() {
            let layer = unsafe { me.layers.get_unchecked_mut(me.idx) };
            let _servers = layer.get_address();
            match ready!(Pin::new(layer).poll_next(cx)) {
                Ok(item) => {
                    // 轮询出已经查到的keys
                    match me.parser.filter_by_key(&me.request, item.iter()) {
                        None => {
                            // 所有请求都已返回
                            me.on_response(item);
                            break;
                        }
                        Some(req) => {
                            me.request = req;
                            me.on_response(item);
                        }
                    }
                }
                Err(e) => {
                    log::debug!("found err: {:?} idx:{}", e, me.idx);
                    last_err = Some(e);
                }
            }

            me.idx += 1;
            if me.idx >= me.layers.len() {
                break;
            }

            if let Err(e) = ready!(me.do_write(cx)) {
                log::warn!("found err when resend layer request:{:?}", e);
                last_err = Some(e);
                break;
            }
        }

        // 先拿走response，然后重置，最后返回响应列表
        me.idx = 0;
        let response = me.response.take();
        let old = std::mem::take(&mut self.request);
        drop(old);
        // 请求完毕，重置
        response
            .map(|item| Poll::Ready(Ok(item)))
            .unwrap_or_else(|| {
                Poll::Ready(Err(last_err.unwrap_or_else(|| {
                    Error::new(
                        ErrorKind::Other,
                        "not error found, may be. layers not configured properly",
                    )
                })))
            })
    }
}

const NAMES: &[&'static str] = &["l0", "l1", "l2", "l3", "l4", "l5", "l6", "l7"];
fn get_name_by_idx(idx: usize) -> &'static str {
    if idx >= NAMES.len() {
        "hit_lunkown"
    } else {
        unsafe { NAMES.get_unchecked(idx) }
    }
}
const NAMES_HIT: &[&'static str] = &[
    "l0_hit_key",
    "l1_hit_key",
    "l2_hit_key",
    "l3_hit_key",
    "l4_hit_key",
    "l5_hit_key",
    "l6_hit_key",
    "l7_hit_key",
];
fn get_key_hit_name_by_idx(idx: usize) -> &'static str {
    if idx >= NAMES_HIT.len() {
        "hit_lunkown"
    } else {
        unsafe { NAMES_HIT.get_unchecked(idx) }
    }
}
