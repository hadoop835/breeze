use crate::Operation;
use ds::RingSlice;

#[derive(Default)]
pub struct Response {
    inner: RingSlice,
    // 包含成功返回的key
    keys: Vec<RingSlice>,
}

impl Response {
    #[inline]
    pub fn from(data: RingSlice, _op: Operation, keys: Vec<RingSlice>) -> Self {
        Self {
            inner: data,
            keys: keys,
        }
    }
    #[inline]
    pub fn keys(&self) -> &[RingSlice] {
        &self.keys
    }
    #[inline]
    pub fn last_key(&self) -> &RingSlice {
        debug_assert!(self.keys.len() > 0);
        unsafe { &self.keys.get_unchecked(self.keys.len() - 1) }
    }
    #[inline]
    pub fn data(&self) -> &RingSlice {
        &self.inner
    }
}
impl AsRef<RingSlice> for Response {
    #[inline(always)]
    fn as_ref(&self) -> &RingSlice {
        &self.inner
    }
}
use std::ops::{Deref, DerefMut};
impl Deref for Response {
    type Target = RingSlice;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for Response {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for Response {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "key len:{} data:{}", self.keys.len(), self.inner)
    }
}
impl Debug for Response {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "key len:{} data:{}", self.keys.len(), self.inner)
    }
}
