use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

use atomic_waker::AtomicWaker;

use crate::{Command, Error, HashedCommand, Parser};
pub struct CallbackContext<E> {
    ctx: u32,
    complete: AtomicBool,
    request: HashedCommand,
    inited: bool, // response是否已经初始化
    response: MaybeUninit<Command>,
    waker: *const AtomicWaker,
    top: *const E,
}

impl<E> CallbackContext<E> {
    #[inline(always)]
    pub fn new(req: HashedCommand, waker: &AtomicWaker) -> Self {
        log::info!("request prepared:{}", req);
        Self {
            request: req,
            waker: waker as *const _,
            complete: AtomicBool::new(false),
            ctx: 0,
            inited: false,
            response: MaybeUninit::uninit(),
        }
    }
}

impl<E> CallbackContext<E> {
    #[inline(always)]
    pub fn on_sent(&mut self) {
        log::info!("request sent: req:{} ", self.request());
    }
    #[inline(always)]
    pub fn on_complete(&mut self, resp: Command) {
        log::info!("on-complete:req:{} resp:{}", self.request(), resp);
        debug_assert!(!self.complete());
        self.write(resp);
        self.complete.store(true, Ordering::Release);
        unsafe { (&*self.waker).wake() }
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
    pub fn as_mut_ptr(&mut self) -> *mut Self {
        self as *mut _
    }
}

impl<E> Drop for CallbackContext<E> {
    #[inline(always)]
    fn drop(&mut self) {
        debug_assert!(self.complete());
        debug_assert!(self.inited);
        unsafe {
            std::ptr::drop_in_place(self.response.as_mut_ptr());
        }
    }
}

unsafe impl<E> Send for CallbackContext<E> {}
unsafe impl<E> Sync for CallbackContext<E> {}
