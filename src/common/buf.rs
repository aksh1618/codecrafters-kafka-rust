use bytes::BufMut;

#[expect(
    clippy::module_name_repetitions,
    reason = "Suitable here as it's an *Ext trait"
)]
pub trait BufMutExt: BufMut {
    fn put_encoded<T: Encode>(&mut self, value: &T) {
        value.encode(self);
    }
}

// Blanket implementation for any type that implements BufMut.
impl<T: BufMut> BufMutExt for T {}

// pub trait BufExt<T> {
//     fn get_api_request_payload(&mut self) -> T;
// }

// TODO: Create derive_macro for this
pub trait Encode {
    fn encode<T: BufMut + ?Sized>(&self, buf: &mut T);
}
