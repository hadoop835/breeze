use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};

use atomic_waker::AtomicWaker;

use protocol::{Command, Error, HashedCommand};
pub struct CallbackContext {
    ctx: u32,
    complete: AtomicBool,
    request: HashedCommand,
    inited: bool, // response是否已经初始化
    response: MaybeUninit<Command>,
    waker: *const AtomicWaker,
}

impl CallbackContext {
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

impl CallbackContext {
    #[inline(always)]
    pub(crate) fn on_sent(&mut self) {
        log::info!("request sent: req:{} ", self.request());
    }
    #[inline(always)]
    pub(crate) fn on_complete(&mut self, resp: Command) {
        log::info!("on-complete:req:{} resp:{}", self.request(), resp);
        debug_assert!(!self.complete());
        self.write(resp);
        self.complete.store(true, Ordering::Release);
        unsafe { (&*self.waker).wake() }
    }
    #[inline(always)]
    pub(crate) fn on_err(&mut self, _err: Error) {
        // let resp = ...;
        //self.on_complete(resp);
        self.complete.store(true, Ordering::Release);
        panic!("on error");
    }
    #[inline(always)]
    pub(crate) fn request(&self) -> &HashedCommand {
        &self.request
    }
    #[inline(always)]
    pub(crate) fn response(&self) -> &Command {
        debug_assert!(self.complete());
        unsafe { self.response.assume_init_ref() }
    }
    #[inline(always)]
    pub(crate) fn complete(&self) -> bool {
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
    pub(crate) fn as_mut_ptr(&mut self) -> *mut Self {
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

//unsafe impl Send for CallbackContextPtr {}
//unsafe impl Sync for CallbackContextPtr {}
//
//pub(crate) struct CallbackContextPtr {
//    ptr: *mut CallbackContext,
//}
//
//impl CallbackContextPtr {
//    #[inline(always)]
//    pub(crate) fn new(mut ctx: CallbackContext) -> Self {
//        let ptr = &mut ctx as *mut CallbackContext;
//        //// drop时释放
//        let _ = std::mem::ManuallyDrop::new(ctx);
//        Self { ptr }
//    }
//    #[inline(always)]
//    pub(crate) fn as_mut_ptr(&mut self) -> *mut CallbackContext {
//        log::info!("as mut ptr:{}", self.ptr as usize);
//        self.ptr
//    }
//}
//
//impl Drop for CallbackContextPtr {
//    #[inline(always)]
//    fn drop(&mut self) {
//        let ptr = self.ptr as *mut CallbackContext;
//        unsafe { std::ptr::drop_in_place(ptr) };
//    }
//}
//
//use std::ops::{Deref, DerefMut};
//impl Deref for CallbackContextPtr {
//    type Target = CallbackContext;
//    #[inline(always)]
//    fn deref(&self) -> &Self::Target {
//        log::info!("deref ptr:{}", self.ptr as usize);
//        unsafe { &*self.ptr }
//    }
//}
//impl DerefMut for CallbackContextPtr {
//    #[inline(always)]
//    fn deref_mut(&mut self) -> &mut Self::Target {
//        log::info!("deref mut ptr:{}", self.ptr as usize);
//        unsafe { &mut *self.ptr }
//    }
//}
