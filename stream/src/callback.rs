use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

use atomic_waker::AtomicWaker;

use crate::{Request, RequestId};
use protocol::{Command, Error, HashedCommand};

pub struct CallbackContext<F: Fn(usize, Request)> {
    pub(crate) ctx: Context,
    request: HashedCommand,
    response: MaybeUninit<Command>,
    waker: *const AtomicWaker,
    id: RequestId,
    receiver: usize,
    callback: F,
}

impl<F> CallbackContext<F> {
    #[inline(always)]
    pub fn new(
        id: RequestId,
        req: HashedCommand,
        waker: &AtomicWaker,
        receiver: usize,
        cb: F,
    ) -> Self
    where
        F: Fn(usize, Request),
    {
        log::info!("request prepared:{}", req);
        Self {
            id,
            ctx: Default::default(),
            waker: waker as *const _,
            request: req,
            response: MaybeUninit::uninit(),
            receiver,
            callback: Box::new(cb),
        }
    }

    #[inline(always)]
    pub fn on_sent(&mut self) {
        log::info!("request sent: {} ", self);
        if self.request().flag().is_sentonly() {
            self.on_done();
        }
    }
    #[inline(always)]
    pub fn on_complete(&mut self, resp: Command) {
        log::info!("on-complete:{} resp:{}", self, resp);
        if !self.ctx.ignore {
            self.write(resp);
        } else {
            drop(resp);
        }
        self.on_done();
    }
    #[inline(always)]
    fn on_done(&mut self) {
        log::info!("on-done:{}", self);
        if self.need_goon() {
            log::info!("on-done-1:{}", self);
            return self.continute();
        }
        log::info!("on-done-2:{}", self);
        if !self.ctx.ignore {
            debug_assert!(!self.complete());
            self.ctx.complete.store(true, Ordering::Release);
            self.wake();
        } else {
            // 只有write_back请求才会ignore
            self.ctx.ignore = false;
            unsafe { std::ptr::drop_in_place(self.as_mut_ptr()) };
        }
    }
    #[inline(always)]
    fn need_goon(&self) -> bool {
        // 1. goon为true
        // 2. 进行回种时或者请求失败
        self.ctx.goon && (self.is_in_async_write_back() || !self.response_ok())
    }
    #[inline(always)]
    fn response_ok(&self) -> bool {
        unsafe { self.ctx.inited && self.response().flag().is_status_ok() }
    }
    #[inline(always)]
    pub fn on_err(&mut self, _err: Error) {
        self.on_done();
    }
    #[inline(always)]
    pub fn request(&self) -> &HashedCommand {
        &self.request
    }
    #[inline(always)]
    pub fn with_request(&mut self, new: HashedCommand) {
        self.request = new;
    }
    // 在使用前，先得判断inited
    #[inline(always)]
    pub unsafe fn response(&self) -> &Command {
        self.response.assume_init_ref()
    }
    #[inline(always)]
    pub fn complete(&self) -> bool {
        self.ctx.complete.load(Ordering::Acquire)
    }
    #[inline(always)]
    fn write(&mut self, resp: Command) {
        debug_assert!(!self.complete());
        self.try_drop_response();
        self.response.write(resp);
        self.ctx.inited = true;
    }
    #[inline(always)]
    fn wake(&self) {
        unsafe { (&*self.waker).wake() }
    }
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut Self {
        self as *mut _
    }
    #[inline(always)]
    pub fn send(&self) {
        self.continute();
    }

    #[inline(always)]
    fn continute(&mut self) {
        let req = self.into();
        (*self.callback)(self.receiver, req);
    }
    #[inline(always)]
    pub fn as_mut_context(&mut self) -> &mut Context {
        &mut self.ctx
    }
    #[inline(always)]
    fn is_in_async_write_back(&self) -> bool {
        self.ctx.ignore
    }
    #[inline(always)]
    fn try_drop_response(&mut self) {
        if self.ctx.inited {
            log::info!("drop response:{}", unsafe { self.response() });
            unsafe { std::ptr::drop_in_place(self.response.as_mut_ptr()) };
            self.ctx.inited = false;
        }
    }
}

impl<F> Drop for CallbackContext<F> {
    #[inline(always)]
    fn drop(&mut self) {
        log::info!("request dropped:{}", self);
        debug_assert!(self.complete());
        debug_assert!(!self.ctx.ignore);
        if self.ctx.inited {
            unsafe {
                std::ptr::drop_in_place(self.response.as_mut_ptr());
            }
        }
    }
}

unsafe impl<F> Send for CallbackContext<F> {}
unsafe impl<F> Sync for CallbackContext<F> {}
#[derive(Default)]
pub struct Context {
    ignore: bool,         // 忽略response，需要手工释放内存
    goon: bool,           // 当前请求可以继续发送下一个请求。
    complete: AtomicBool, // 当前请求是否完成
    inited: bool,         // response是否已经初始化
    write_back: bool,     // 请求结束后，是否需要回写。
    flag: protocol::Context,
}

impl Context {
    #[inline(always)]
    pub fn as_mut_flag(&mut self) -> &mut protocol::Context {
        &mut self.flag
    }
    #[inline(always)]
    pub fn goon(&mut self, goon: bool) {
        self.goon = goon;
    }
    #[inline(always)]
    pub fn write_back(&mut self, wb: bool) {
        self.write_back = wb;
    }
    #[inline(always)]
    pub fn is_write_back(&self) -> bool {
        self.write_back
    }
    #[inline(always)]
    pub fn is_inited(&self) -> bool {
        self.inited
    }
}

impl Into<Request> for &mut CallbackContext {
    #[inline(always)]
    fn into(self) -> Request {
        let ctx = self.as_mut_ptr();
        Request::new(ctx)
    }
}
use std::fmt::{self, Debug, Display, Formatter};
impl Display for CallbackContext {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.ctx, self.request())
    }
}
impl Debug for CallbackContext {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for Context {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "complete:{} init:{} ignore:{}, goon:{} write back:{} context:{}",
            self.complete.load(Ordering::Relaxed),
            self.inited,
            self.ignore,
            self.goon,
            self.write_back,
            self.flag
        )
    }
}

use std::ptr::NonNull;
pub struct CallbackContextPtr {
    inner: NonNull<CallbackContext>,
}

impl CallbackContextPtr {
    #[inline]
    pub fn build_request(&mut self) -> Request {
        unsafe { Request::new(self.inner.as_mut()) }
    }
    // 调用完这个方法后，不会再关注response的值。
    #[inline(always)]
    pub fn async_start_write_back(&mut self) {
        debug_assert!(self.complete());
        debug_assert!(self.ctx.is_write_back());
        debug_assert!(self.ctx.inited);
        unsafe {
            let ctx = self.inner.as_mut();
            ctx.try_drop_response();
            // write_back请求是异步的，不需要response
            //需要在on_done时主动销毁self对象
            ctx.ctx.ignore = true;
            log::info!("start write back:{}", self.inner.as_ref());
            ctx.continute();
        }
    }
}

impl From<CallbackContext> for CallbackContextPtr {
    #[inline]
    fn from(ctx: CallbackContext) -> Self {
        let ptr = Box::leak(Box::new(ctx));
        let inner = unsafe { NonNull::new_unchecked(ptr) };
        Self { inner }
    }
}

impl Drop for CallbackContextPtr {
    #[inline]
    fn drop(&mut self) {
        // 如果ignore为true，说明当前内存手工释放
        unsafe {
            if !self.inner.as_ref().ctx.ignore {
                Box::from_raw(self.inner.as_ptr());
            }
        }
    }
}
use std::ops::{Deref, DerefMut};
impl Deref for CallbackContextPtr {
    type Target = CallbackContext;
    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe {
            debug_assert!(!self.inner.as_ref().ctx.ignore);
            self.inner.as_ref()
        }
    }
}
impl DerefMut for CallbackContextPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            debug_assert!(!self.inner.as_ref().ctx.ignore);
            self.inner.as_mut()
        }
    }
}
unsafe impl Send for CallbackContextPtr {}
unsafe impl Sync for CallbackContextPtr {}
