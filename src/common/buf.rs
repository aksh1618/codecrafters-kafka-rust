use bytes::{Buf, BufMut};

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
    fn encode<B: BufMut + ?Sized>(&self, buf: &mut B);
}

#[expect(
    clippy::module_name_repetitions,
    reason = "Suitable here as it's an *Ext trait"
)]
pub trait BufExt: Buf {
    fn get_decoded<T: Decode>(&mut self) -> T {
        Decode::decode(self)
    }
}

// Blanket implementation for any type that implements Buf.
impl<T: Buf> BufExt for T {}

pub trait Decode {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self;
}
