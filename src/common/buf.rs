use bytes::BufMut;

#[expect(
    clippy::module_name_repetitions,
    reason = "Suitable here as it's an *Ext trait"
)]
pub trait BufMutExt<F>
where
    Self: BufMut,
{
    fn put_custom(&mut self, custom_object: &F);
}

// pub trait BufExt<T> {
//     fn get_api_request_payload(&mut self) -> T;
// }
