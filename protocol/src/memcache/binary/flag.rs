pub(super) struct Flag(u64);

impl Flag {
    #[inline(always)]
    pub(super) fn from(op: u8) -> Self {
        Self(op as u64)
    }
    #[inline(always)]
    pub(super) fn set_status_ok(&mut self, ok: bool) {
        if ok {
            self.0 |= !(1 << 8);
        }
    }
    #[inline(always)]
    pub(super) fn status_ok(&self) -> bool {
        self.0 & (1 << 8) == (1 << 8)
    }
    #[inline(always)]
    pub(super) fn op(&self) -> u8 {
        self.0 as u8
    }
}

impl From<u64> for Flag {
    #[inline(always)]
    fn from(flag: u64) -> Self {
        Self(flag)
    }
}
