use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

use atomic_waker::AtomicWaker;

use crate::Request;
use protocol::{Command, Error, HashedCommand};

pub struct CallbackContext {
    ctx: u32,
    complete: AtomicBool, // 当前请求是否完成
    inited: bool,         // response是否已经初始化
    // 当前请求所属的上下文是否仍然有效。
    // 部分情况下，因为异常退出，导致上下文失效，此时请求结束时：
    // 1. 不需要再进行回调；
    // 2. 手动销毁当前对象。
    valid: AtomicBool,
    request: HashedCommand,
    response: MaybeUninit<Command>,
    waker: *const AtomicWaker,
    on_not_ok: Box<dyn Fn(Request)>,
}

impl CallbackContext {
    #[inline(always)]
    pub fn new<F: Fn(Request) + 'static>(
        req: HashedCommand,
        waker: &AtomicWaker,
        not_ok: F,
    ) -> Self {
        log::info!("request prepared:{}", req);
        Self {
            ctx: 0,
            waker: waker as *const _,
            complete: AtomicBool::new(false),
            inited: false,
            valid: AtomicBool::new(true),
            request: req,
            response: MaybeUninit::uninit(),
            on_not_ok: Box::new(not_ok),
        }
    }
}

impl CallbackContext {
    #[inline(always)]
    pub fn invalid(&self) {
        assert_eq!(self.is_valid(), true);
        self.valid.store(false, Ordering::Release);
    }
    #[inline(always)]
    fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }
    #[inline(always)]
    pub fn on_sent(&mut self) {
        log::info!("request sent: req:{} ", self.request());
    }
    #[inline(always)]
    pub fn on_complete(&mut self, resp: Command) {
        log::info!("on-complete:req:{} resp:{}", self.request(), resp);
        debug_assert!(!self.complete());
        //if resp.flag().is_status_ok() {
        self.write(resp);
        self.complete.store(true, Ordering::Release);
        self.wake();
        //} else {
        //    if !self.inited {
        //        self.write(resp);
        //    }
        //    self.on_not_ok();
        //}
    }
    #[inline(always)]
    fn on_not_ok(&mut self) {
        let req = Request::new(self as *mut _);
        (*self.on_not_ok)(req)
    }
    #[inline(always)]
    pub fn on_err(&mut self, _err: Error) {
        self.complete.store(true, Ordering::Release);
    }
    #[inline(always)]
    pub fn request(&self) -> &HashedCommand {
        &self.request
    }
    #[inline(always)]
    pub fn response(&self) -> &Command {
        debug_assert!(self.complete());
        unsafe { self.response.assume_init_ref() }
    }
    #[inline(always)]
    pub fn complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    #[inline(always)]
    fn write(&mut self, resp: Command) {
        debug_assert!(!self.complete());
        unsafe {
            if self.inited {
                std::ptr::drop_in_place(self.response.as_mut_ptr());
            }
            self.inited = true;
            self.response.write(resp);
        }
    }
    #[inline(always)]
    fn wake(&self) {
        unsafe { (&*self.waker).wake() }
    }

    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut Self {
        self as *mut _
    }
}

impl Drop for CallbackContext {
    #[inline(always)]
    fn drop(&mut self) {
        debug_assert!(self.complete());
        debug_assert!(self.inited);
        unsafe {
            std::ptr::drop_in_place(self.response.as_mut_ptr());
        }
    }
}

unsafe impl Send for CallbackContext {}
unsafe impl Sync for CallbackContext {}
