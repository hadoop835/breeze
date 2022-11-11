use ds::time::Instant;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ds::AtomicWaker;

use crate::request::Request;
use crate::{Command, Error, HashedCommand};

pub struct Callback {
    exp_sec: Box<dyn Fn() -> u32>,
    cb: Box<dyn Fn(Request)>,
}
impl Callback {
    #[inline]
    pub fn new(cb: Box<dyn Fn(Request)>, exp_sec: Box<dyn Fn() -> u32>) -> Self {
        Self { cb, exp_sec }
    }
    #[inline]
    pub fn send(&self, req: Request) {
        log::debug!("request sending:{}", req);
        (self.cb)(req);
    }
    #[inline]
    pub fn exp_sec(&self) -> u32 {
        (self.exp_sec)()
    }
}

// 在同步调用中设置waker
// 在异步调用中设置on_drop
union WakerDrop {
    waker: *const AtomicWaker,
    async_pending: *const AtomicUsize,
}

pub struct CallbackContext {
    pub(crate) ctx: Context,
    request: HashedCommand,
    response: MaybeUninit<Command>,
    callback: CallbackPtr,
    start: Instant, // 请求的开始时间
    tries: u8,
    atomic: WakerDrop,
    //waker: *const AtomicWaker,
    //on_drop: *const AtomicUsize,
}

impl CallbackContext {
    #[inline]
    pub fn new(
        req: HashedCommand,
        waker: &AtomicWaker,
        cb: CallbackPtr,
        first: bool,
        last: bool,
    ) -> Self {
        let mut ctx = Context::default();
        ctx.first = first;
        ctx.last = last;
        log::debug!("request prepared:{}", req);
        Self {
            ctx,
            request: req,
            response: MaybeUninit::uninit(),
            callback: cb,
            start: Instant::now(),
            tries: 0,
            atomic: WakerDrop { waker },
        }
    }
    #[inline]
    pub(crate) fn on_noforward(&mut self) {
        assert!(
            self.request().noforward(),
            "req: {:?}",
            self.request().data()
        );
        self.on_done();
    }

    // 返回true: 表示发送完之后还未结束
    // false: 表示请求已结束
    #[inline]
    pub(crate) fn on_sent(&mut self) -> bool {
        log::debug!("request sent: {} ", self);
        if self.request().sentonly() {
            self.on_done();
            false
        } else {
            true
        }
    }
    #[inline]
    pub fn on_complete(&mut self, resp: Command) {
        log::debug!("on-complete:{} resp:{}", self, resp);
        // 异步请求不关注response。
        if !self.ctx.async_mode {
            self.tries += 1;
            assert!(
                !self.complete(),
                "{} {:?} => {:?}",
                self,
                self.request().data(),
                resp.data()
            );
            self.try_drop_response();
            self.response.write(resp);
            self.ctx.inited.store(true, Ordering::Release);
        }
        self.on_done();
    }
    #[inline]
    fn on_done(&mut self) {
        log::debug!("on-done:{}", self);
        if self.need_goon() {
            return self.goon();
        }
        if !self.ctx.async_mode {
            // 说明有请求在pending
            assert!(!self.complete(), "req:{:?}", self.request().data());
            self.ctx.complete.store(true, Ordering::Release);
            unsafe { (&*self.atomic.waker).wake() }
        } else {
            // async_mode需要手动释放
            self.manual_drop();
        }
    }
    #[inline]
    fn need_goon(&self) -> bool {
        if !self.ctx.async_mode {
            // 正常访问请求。
            // old: 除非出现了error，否则最多只尝试一次;
            // 为了提升mcq的读写效率，tries改为3次
            self.ctx.try_next && !self.response_ok() && self.tries < 3
        } else {
            // write back请求
            self.ctx.write_back
        }
    }
    #[inline]
    pub fn response_ok(&self) -> bool {
        unsafe { self.inited() && self.unchecked_response().ok() }
    }
    #[inline]
    pub fn on_err(&mut self, err: Error) {
        // 正常err场景，仅仅在debug时check
        log::debug!("++++  hanle found err: {:?}", err);
        match err {
            Error::Closed => {}
            Error::ChanDisabled => {}
            Error::Waiting => {}
            Error::Pending => {}
            _err => log::debug!(
                "on-err:{} {:?} request:{:?}",
                self,
                self.request().data(),
                _err
            ),
        }
        self.on_done();
    }
    #[inline]
    pub fn request(&self) -> &HashedCommand {
        &self.request
    }
    #[inline]
    pub fn request_mut(&mut self) -> &mut HashedCommand {
        &mut self.request
    }
    // 在使用前，先得判断inited
    #[inline]
    pub unsafe fn unchecked_response(&self) -> &Command {
        assert!(self.inited());
        self.response.assume_init_ref()
    }
    #[inline]
    pub fn complete(&self) -> bool {
        self.ctx.complete.load(Ordering::Acquire)
    }
    #[inline]
    pub fn inited(&self) -> bool {
        self.ctx.is_inited()
    }
    #[inline]
    pub fn is_write_back(&self) -> bool {
        self.ctx.write_back
    }
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut Self {
        self as *mut _
    }
    #[inline]
    pub fn send(&mut self) {
        let req = Request::new(self.as_mut_ptr());
        (*self.callback).send(req);
    }
    #[inline]
    pub fn start_at(&self) -> Instant {
        self.start
    }

    #[inline]
    fn goon(&mut self) {
        self.send();
    }
    #[inline]
    pub fn as_mut_context(&mut self) -> &mut Context {
        &mut self.ctx
    }
    #[inline]
    fn try_drop_response(&mut self) {
        if self.ctx.is_inited() {
            log::debug!("drop response:{}", unsafe { self.unchecked_response() });
            self.ctx
                .inited
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
                .expect("cas failed");
            unsafe { std::ptr::drop_in_place(self.response.as_mut_ptr()) };
        }
    }
    #[inline]
    pub fn first(&self) -> bool {
        self.ctx.first
    }
    #[inline]
    pub fn last(&self) -> bool {
        self.ctx.last
    }
    #[inline]
    fn manual_drop(&mut self) {
        unsafe { Box::from_raw(self) };
    }
}

impl Drop for CallbackContext {
    #[inline]
    fn drop(&mut self) {
        assert!(self.complete());
        self.try_drop_response();
        if self.ctx.async_mode {
            unsafe { (&*self.atomic.async_pending).fetch_sub(1, Ordering::AcqRel) };
        }
    }
}

unsafe impl Send for CallbackContext {}
unsafe impl Sync for CallbackContext {}
#[derive(Default)]
pub struct Context {
    complete: AtomicBool, // 当前请求是否完成
    inited: AtomicBool,   // response是否已经初始化
    async_mode: bool,     // 是否是异步请求
    try_next: bool,       // 请求失败是否需要重试
    write_back: bool,     // 请求结束后，是否需要回写。
    first: bool,          // 当前请求是否是所有子请求的第一个
    last: bool,           // 当前请求是否是所有子请求的最后一个
    _padding: bool,
    flag: crate::Context,
}

impl Context {
    #[inline]
    pub fn as_mut_flag(&mut self) -> &mut crate::Context {
        &mut self.flag
    }
    #[inline]
    pub fn try_next(&mut self, goon: bool) {
        self.try_next = goon;
    }
    #[inline]
    pub fn write_back(&mut self, wb: bool) {
        self.write_back = wb;
    }
    #[inline]
    pub fn is_write_back(&self) -> bool {
        self.write_back
    }
    #[inline]
    pub fn is_inited(&self) -> bool {
        self.inited.load(Ordering::Acquire)
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for CallbackContext {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} tries:{}", self.ctx, self.request(), self.tries)
    }
}
impl Debug for CallbackContext {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for Context {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "complete:{} init:{},async :{} try:{} write back:{}  context:{}",
            self.complete.load(Ordering::Acquire),
            self.is_inited(),
            self.async_mode,
            self.try_next,
            self.write_back,
            self.flag,
        )
    }
}

pub struct CallbackContextPtr {
    ptr: *mut CallbackContext,
}

impl CallbackContextPtr {
    #[inline]
    pub fn build_request(&mut self) -> Request {
        Request::new(self.ptr)
    }
    //需要在on_done时主动销毁self对象
    #[inline]
    pub fn async_write_back<P: crate::Protocol>(mut self, parser: &P, async_pending: &AtomicUsize) {
        assert!(self.inited() && self.complete(), "cbptr:{:?}", &*self);
        let mut ctx = unsafe { Box::from_raw(self.ptr) };
        ctx.ctx.async_mode = true;
        ctx.atomic.async_pending = async_pending;
        let exp = ctx.callback.exp_sec();
        if let Some(new) = parser.build_writeback_request(&mut self, exp) {
            self.request = new;
        }
        log::debug!("start write back:{}", ctx);
        let mut async_ctx: MaybeUninit<CallbackContext> = MaybeUninit::uninit();
        let async_ctx = unsafe { Box::leak(Box::from_raw(async_ctx.write(*ctx))) };
        // response已经释放
        *async_ctx.ctx.inited.get_mut() = true;

        async_ctx.goon();
    }
}

impl From<CallbackContext> for CallbackContextPtr {
    #[inline]
    fn from(ctx: CallbackContext) -> Self {
        let ptr = Box::leak(Box::new(ctx));
        Self { ptr }
    }
}

impl Drop for CallbackContextPtr {
    #[inline]
    fn drop(&mut self) {
        // 如果ignore为true，说明当前内存手工释放
        if !self.ctx.async_mode {
            self.manual_drop();
        }
    }
}
use std::ops::{Deref, DerefMut};
impl Deref for CallbackContextPtr {
    type Target = CallbackContext;
    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}
impl DerefMut for CallbackContextPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.ptr }
    }
}
unsafe impl Send for CallbackContextPtr {}
unsafe impl Sync for CallbackContextPtr {}
unsafe impl Send for CallbackPtr {}
unsafe impl Sync for CallbackPtr {}
unsafe impl Send for Callback {}
unsafe impl Sync for Callback {}
#[derive(Clone)]
pub struct CallbackPtr {
    ptr: *const Callback,
}
impl Deref for CallbackPtr {
    type Target = Callback;
    #[inline]
    fn deref(&self) -> &Self::Target {
        assert!(!self.ptr.is_null());
        unsafe { &*self.ptr }
    }
}
impl From<&Callback> for CallbackPtr {
    // 调用方确保CallbackPtr在使用前，指针的有效性。
    fn from(cb: &Callback) -> Self {
        Self {
            ptr: cb as *const _,
        }
    }
}

impl crate::Commander for CallbackContextPtr {
    #[inline]
    fn request_mut(&mut self) -> &mut HashedCommand {
        &mut self.request
    }
    #[inline]
    fn request(&self) -> &HashedCommand {
        &self.request
    }
    #[inline]
    fn response(&self) -> &Command {
        assert!(self.inited());
        unsafe { self.unchecked_response() }
    }
    #[inline]
    fn response_mut(&mut self) -> &mut Command {
        assert!(self.inited());
        unsafe { self.response.assume_init_mut() }
    }
}
