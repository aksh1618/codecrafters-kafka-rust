#![allow(
    dead_code,
    reason = "Going to evolve this as we go, so allowing some dead code for now"
)]

use std::{io, result};

use bytes::{BufMut as _, Bytes, BytesMut};
use strum::Display;
use thiserror::Error;

use crate::buf::BufMutExt;

pub struct RequestMessageV2 {
    pub header: RequestHeaderV2,
    pub payload: RequestPayload,
}

// TODO: Is this beneficial? Should we use newtype instead of alias?
pub type RequestPayload = Bytes;
pub type ResponsePayload = Bytes;
// pub type ResponsePayload<T: BufMut> = T;
// pub struct ResponsePayload<T: BufMut>(T);

#[derive(Debug, Default)]
pub struct RequestHeaderV2 {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
}

// pub trait RequestPayload<T> {
//     fn get_api_request_payload(&mut self) -> T;
// }
//
// impl RequestPayload<ApiVersionsRequest> for Vec<u8> {
//     fn get_api_request_payload(&mut self) -> ApiVersionsRequest {
//         todo!()
//     }
// }

// TODO: Is this beneficial? Should we use newtype instead of alias?
pub type ApiKey = i16;
pub type ApiVersion = i16;
pub type CorrelationId = i32;

pub struct SupportedVersions {
    pub min_version: ApiVersion,
    pub max_version: ApiVersion,
}

pub struct RequestV2 {
    pub size: i32,
    pub message: RequestMessageV2,
}

pub struct Response {
    pub message_size: i32,
    pub message: ResponsePayload,
}

/// Error codes from the Kafka Protocol
///
/// > We use numeric codes to indicate what problem occurred on the server. These can be translated
/// > by the client into exceptions or whatever the appropriate error handling mechanism in the
/// > client language.
/// > (From [Kafka protocol doc](https://kafka.apache.org/protocol.html#protocol_error_codes))
#[repr(i16)]
#[derive(Display, Debug, Default, Copy, Clone, Error)]
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

// TODO: Make this actually unsigned varint
pub type UnsignedVarint = u8;

#[derive(Default)]
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

impl<Item> BufMutExt<CompactArray<Item>> for BytesMut
where
    Self: BufMutExt<Item>,
{
    fn put_custom(&mut self, compact_array: &CompactArray<Item>) {
        self.put_u8(compact_array.length + 1);
        for x in &compact_array.elements {
            self.put_custom(x);
        }
    }
}

pub type Int32 = i32;

// TODO: Make this actually TagBuffer, see https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=120722234#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-Serialization
pub type TagBuffer = i8;

// impl<T: BufMut, Item> BufMutExt<CompactArray<Item>> for T
// where
//     for<'a> &'a mut T: BufMutExt<Item>,
// {
//     fn put_api_response_payload(&mut self, api_spec: &CompactArray<Item>) {
//         self.put_u8(api_spec.length);
//         for x in &api_spec.elements {
//             self.put_api_response_payload(x);
//         }
//         // api_spec
//         //     .elements
//         //     .into_iter()
//         //     .for_each(|x| BufMutExt::put_api_response_payload(&mut self, &x));
//     }
// }
