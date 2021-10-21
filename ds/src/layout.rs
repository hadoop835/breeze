cfg_if::cfg_if! {
if #[cfg(feature = "single-thread")] {
    pub struct CacheAligned<T: Sized>(pub T);
    impl<T> CacheAligned<T> {
        pub fn new(t: T) -> Self {
            Self(t)
        }
    }
} else {
    pub use layout::CacheAligned;
}}

impl<T: Default> Default for CacheAligned<T> {
    #[inline]
    fn default() -> Self {
        CacheAligned::new(T::default())
    }
}
