pub trait Endpoint: Sized {
    type Item;
    fn send(&self, req: Self::Item);
}

impl<T, R> Endpoint for &T
where
    T: Endpoint<Item = R>,
{
    type Item = R;
    #[inline(always)]
    fn send(&self, req: R) {
        (*self).send(req)
    }
}

impl<T, R> Endpoint for std::sync::Arc<T>
where
    T: Endpoint<Item = R>,
{
    type Item = R;
    #[inline(always)]
    fn send(&self, req: R) {
        (**self).send(req)
    }
}
