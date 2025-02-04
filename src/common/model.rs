#![allow(
    dead_code,
    reason = "Going to evolve this as we go, so allowing some dead code for now"
)]

use std::{io, result};

use bytes::{BufMut, Bytes, BytesMut};
use encode_derive::Encode;
use paste::paste;
use strum::Display;
use thiserror::Error;

use super::buf::BufMutExt as _;
use super::buf::Encode;

/// Error codes from the Kafka Protocol
///
/// > We use numeric codes to indicate what problem occurred on the server. These can be translated
/// > by the client into exceptions or whatever the appropriate error handling mechanism in the
/// > client language.
/// > (From [Kafka protocol doc](https://kafka.apache.org/protocol.html#protocol_error_codes))
#[repr(i16)]
#[derive(Display, Debug, Default, Copy, Clone, Error, Encode)]
pub enum ErrorCode {
    UnknownServerError = -1,
    #[default]
    None = 0,
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

