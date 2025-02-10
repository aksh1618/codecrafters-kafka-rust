#![allow(
    dead_code,
    reason = "Going to evolve this as we go, so allowing some dead code for now"
)]

use std::{io, result};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use encode_decode_derive::{Decode, Encode};
use paste::paste;
use smart_default::SmartDefault;
use strum::{Display, FromRepr};
use thiserror::Error;

use super::buf::{BufExt as _, BufMutExt as _, Decode, Encode};

/// Error codes from the Kafka Protocol
///
/// > We use numeric codes to indicate what problem occurred on the server. These can be translated
/// > by the client into exceptions or whatever the appropriate error handling mechanism in the
/// > client language.
/// > (From [Kafka protocol doc](https://kafka.apache.org/protocol.html#protocol_error_codes))
#[repr(i16)]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Error, Display, FromRepr, Encode, Decode)]
pub enum ErrorCode {
    UnknownServerError = -1,
    #[default]
    None = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
    InvalidRequest = 42,
    // TODO: Add more from https://kafka.apache.org/protocol.html#protocol_error_codes
}

pub type Result<T> = result::Result<T, ErrorCode>;

impl From<ErrorCode> for Bytes {
    fn from(error_code: ErrorCode) -> Self {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_i16(error_code as i16);
        buf.freeze()
    }
}

impl From<io::Error> for ErrorCode {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::InvalidData {
            Self::InvalidRequest
        } else {
            Self::UnknownServerError
        }
    }
}

#[derive(Debug, Default)]
pub struct CompactArray<Item> {
    pub length: UnsignedVarint,
    pub elements: Vec<Item>,
}

// TODO: Add null array case
impl<T> From<Vec<T>> for CompactArray<T> {
    fn from(value: Vec<T>) -> Self {
        Self {
            length: UnsignedVarint::try_from(value.len()).expect(
                "Arrays should be smaller than i8::max in length, probably time to refactor",
            ),
            elements: value,
        }
    }
}

impl<Item: Encode> Encode for CompactArray<Item> {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        buf.put_u8(self.length + 1);
        for x in &self.elements {
            buf.put_encoded(x);
        }
    }
}

impl<Item: Decode> Decode for CompactArray<Item> {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        let length = buf.get_u8().saturating_sub(1);
        let mut elements = Vec::with_capacity(length as usize);
        for _ in 0..length {
            elements.push(buf.get_decoded());
        }
        Self { length, elements }
    }
}

pub type Boolean = bool;
impl Encode for Boolean {
    fn encode<T: BufMut + ?Sized>(&self, buf: &mut T) {
        buf.put_u8(u8::from(*self));
    }
}
impl Decode for Boolean {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        buf.get_u8() != 0
    }
}

pub type Uuid = uuid::Uuid;
impl Encode for Uuid {
    fn encode<T: BufMut + ?Sized>(&self, buf: &mut T) {
        buf.put_slice(self.as_bytes());
    }
}
impl Decode for Uuid {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        let mut bytes = [0; 16];
        buf.copy_to_slice(&mut bytes);
        Self::from_bytes(bytes)
    }
}

// TODO: Make this actually unsigned varint
pub type UnsignedVarint = u8;
// TODO: Make this actually TagBuffer, see https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=120722234#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-Serialization
pub type TagBuffer = i8;
// TODO: Is this beneficial? Should we use newtype instead of alias?
pub type ApiKey = i16;
pub type ApiVersion = i16;
pub type CorrelationId = i32;
pub type Int16 = i16;
pub type Int32 = i32;
pub type Int64 = i64;

macro_rules! impl_encode_int {
    ($ty:ty) => {
        paste! {
            impl Encode for $ty {
                fn encode<T: BufMut + ?Sized>(&self, buf: &mut T) {
                    buf.[<put_$ty>](*self);
                }
            }
        }
    };
}

impl_encode_int!(u8);
impl_encode_int!(i8);
impl_encode_int!(i16);
impl_encode_int!(i32);
impl_encode_int!(i64);

macro_rules! impl_decode_int {
    ($ty:ty) => {
        paste! {
            impl Decode for $ty {
                fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
                    buf.[<get_$ty>]()
                }
            }
        }
    };
}

impl_decode_int!(u8);
impl_decode_int!(i8);
impl_decode_int!(i16);
impl_decode_int!(i32);
impl_decode_int!(i64);

#[derive(Debug, Default)]
pub struct CompactString {
    pub length: UnsignedVarint,
    pub value: Option<Bytes>,
}

impl Encode for CompactString {
    fn encode<B: BufMut + ?Sized>(&self, mut buf: &mut B) {
        buf.put_encoded(&(self.length + 1));
        buf.put_encoded(&self.value);
    }
}

impl Decode for CompactString {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        let length = buf.get_u8().saturating_sub(1);
        if length == 0 {
            return Self {
                length: 0,
                value: Some(Bytes::new()),
            };
        }
        let value = Some(buf.take(length.into()).get_decoded());
        Self { length, value }
    }
}

impl From<&str> for CompactString {
    fn from(value: &str) -> Self {
        Self {
            length: UnsignedVarint::try_from(value.len()).expect(
                "Strings should be smaller than u8::max in length, probably time to refactor",
            ),
            value: Some(Bytes::from(value.to_string())),
        }
    }
}

#[derive(Debug, Encode, SmartDefault)]
pub struct NullableString {
    #[default(-1_i16)]
    length: i16,
    value: Option<Bytes>,
}

impl NullableString {
    pub fn encoded_size(&self) -> usize {
        size_of_val(&self.length) + self.value.as_ref().map(Bytes::len).unwrap_or(0)
    }
}

impl Decode for NullableString {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        let length = buf.get_i16();
        if length == -1 {
            return Self {
                length,
                value: None,
            };
        }
        let string_length = length.try_into().expect("length should be non-negative");
        let value = Some(buf.take(string_length).get_decoded());
        Self { length, value }
    }
}

impl From<&str> for NullableString {
    fn from(value: &str) -> Self {
        Self {
            length: i16::try_from(value.len())
                .expect("Nullable string should be smaller than i16::max in length"),
            value: Some(Bytes::from(value.to_string())),
        }
    }
}

#[derive(Debug, Default)]
pub struct CompactNullableString {
    pub length: UnsignedVarint,
    pub value: Option<Bytes>,
}

impl Encode for CompactNullableString {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        buf.put_u8(self.length + 1);
        buf.put_encoded(&self.value);
    }
}

impl Decode for CompactNullableString {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        let length = buf.get_u8().saturating_sub(1);
        if length == 0 {
            return Self {
                length: 0,
                value: None,
            };
        }
        let value = Some(buf.take(length.into()).get_decoded());
        Self { length, value }
    }
}

impl Encode for Option<Bytes> {
    fn encode<B: BufMut + ?Sized>(&self, mut buf: &mut B) {
        if let Some(x) = self {
            buf.put_encoded(x);
        }
    }
}

impl Encode for Bytes {
    fn encode<B: BufMut + ?Sized>(&self, buf: &mut B) {
        buf.put_slice(self);
    }
}

impl Decode for Bytes {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        buf.copy_to_bytes(buf.remaining())
    }
}
