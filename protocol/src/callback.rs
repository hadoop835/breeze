use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

use ds::AtomicWaker;

use crate::request::Request;
use crate::{Command, Error, HashedCommand};

pub struct Callback {
    receiver: usize,
    cb: fn(usize, Request),
}
impl Callback {
    #[inline]
    pub fn new(receiver: usize, cb: fn(usize, Request)) -> Self {
        Self { receiver, cb }
    }
    #[inline(always)]
    pub fn send(&self, req: Request) {
        (self.cb)(self.receiver, req);
    }
}

pub struct CallbackContext {
    pub(crate) ctx: Context,
    request: HashedCommand,
    response: MaybeUninit<Command>,
    waker: *const AtomicWaker,
    callback: CallbackPtr,
}

impl CallbackContext {
    #[inline(always)]
    pub fn new(req: HashedCommand, waker: &AtomicWaker, cb: CallbackPtr) -> Self {
        log::debug!("request prepared:{}", req);
        Self {
            ctx: Default::default(),
            waker: waker as *const _,
            request: req,
            response: MaybeUninit::uninit(),
            callback: cb,
        }
    }

    #[inline(always)]
    pub fn on_sent(&mut self) {
        log::debug!("request sent: {} ", self);
        if self.request().flag().is_sentonly() {
            self.on_done();
        }
    }
    #[inline(always)]
    pub fn on_complete(&mut self, resp: Command) {
        log::debug!("on-complete:{} resp:{}", self, resp);
        if !self.ctx.ignore {
            self.write(resp);
        } else {
            drop(resp);
        }
        self.on_done();
    }
    #[inline(always)]
    pub fn on_response(&self) {}
    #[inline(always)]
    fn on_done(&mut self) {
        if self.need_goon() {
            return self.continute();
        }
        if !self.ctx.ignore {
            debug_assert!(!self.complete());
            self.ctx.complete.store(true, Ordering::Release);
            self.wake();
        } else {
            // 只有write_back请求才会ignore
            self.ctx.ignore = false;
            unsafe { std::ptr::drop_in_place(self.as_mut_ptr()) };
        }
        log::debug!("on-done:{}", self);
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
    pub fn on_err(&mut self, err: Error) {
        log::debug!("on-err:{} {}", self, err);
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
    pub fn inited(&self) -> bool {
        self.ctx.inited
    }
    #[inline(always)]
    pub fn is_write_back(&self) -> bool {
        self.ctx.write_back
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
    pub fn start(&mut self) {
        self.send();
    }
    #[inline(always)]
    fn send(&mut self) {
        let req = Request::new(self.as_mut_ptr());
        (*self.callback).send(req);
    }

    #[inline(always)]
    fn continute(&mut self) {
        self.send();
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
            log::debug!("drop response:{}", unsafe { self.response() });
            unsafe { std::ptr::drop_in_place(self.response.as_mut_ptr()) };
            self.ctx.inited = false;
        }
    }
}

impl Drop for CallbackContext {
    #[inline(always)]
    fn drop(&mut self) {
        log::debug!("request dropped:{}", self);
        //debug_assert!(self.complete());
        debug_assert!(!self.ctx.ignore);
        if self.ctx.inited {
            unsafe {
                std::ptr::drop_in_place(self.response.as_mut_ptr());
            }
        }
    }
}

unsafe impl Send for CallbackContext {}
unsafe impl Sync for CallbackContext {}
#[derive(Default)]
pub struct Context {
    ignore: bool,         // 忽略response，需要手工释放内存
    goon: bool,           // 当前请求可以继续发送下一个请求。
    complete: AtomicBool, // 当前请求是否完成
    inited: bool,         // response是否已经初始化
    write_back: bool,     // 请求结束后，是否需要回写。
    flag: crate::Context,
}

impl Context {
    #[inline(always)]
    pub fn as_mut_flag(&mut self) -> &mut crate::Context {
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
    #[inline(always)]
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
            log::debug!("start write back:{}", self.inner.as_ref());
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
unsafe impl Send for CallbackPtr {}
unsafe impl Sync for CallbackPtr {}
#[derive(Clone)]
pub struct CallbackPtr {
    ptr: *const Callback,
}
impl Deref for CallbackPtr {
    type Target = Callback;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        debug_assert!(!self.ptr.is_null());
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
