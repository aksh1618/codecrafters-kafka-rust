#![allow(dead_code)]

use std::{io, result};

use bytes::{BufMut as _, Bytes, BytesMut};
use strum::Display;
use thiserror::Error;

use crate::buf::BufMutExt;

pub(crate) struct RequestMessageV2 {
    pub(crate) header: RequestHeaderV2,
    pub(crate) payload: RequestPayload,
}

// TODO: Is this beneficial? Should we use newtype instead of alias?
pub(crate) type RequestPayload = Bytes;
pub(crate) type ResponsePayload = Bytes;
// pub(crate) type ResponsePayload<T: BufMut> = T;
// pub(crate) struct ResponsePayload<T: BufMut>(T);

#[derive(Debug)]
pub(crate) struct RequestHeaderV2 {
    pub(crate) api_key: ApiKey,
    pub(crate) api_version: ApiVersion,
    pub(crate) correlation_id: CorrelationId,
}

// pub(crate) trait RequestPayload<T> {
//     fn get_api_request_payload(&mut self) -> T;
// }
//
// impl RequestPayload<ApiVersionsRequest> for Vec<u8> {
//     fn get_api_request_payload(&mut self) -> ApiVersionsRequest {
//         todo!()
//     }
// }

// TODO: Is this beneficial? Should we use newtype instead of alias?
pub(crate) type ApiKey = i16;
pub(crate) type ApiVersion = i16;
pub(crate) type CorrelationId = i32;

pub(crate) struct SupportedVersions {
    pub(crate) min_version: ApiVersion,
    pub(crate) max_version: ApiVersion,
}

pub(crate) struct RequestV2 {
    pub(crate) size: i32,
    pub(crate) message: RequestMessageV2,
}

pub(crate) struct Response {
    pub(crate) message_size: i32,
    pub(crate) message: ResponsePayload,
}

/// We use numeric codes to indicate what problem occurred on the server. These can be translated
/// by the client into exceptions or whatever the appropriate error handling mechanism in the
/// client language.
///
/// (Copied form [kafka protocol doc](https://kafka.apache.org/protocol.html#protocol_error_codes))
#[repr(i16)]
#[derive(Display, Debug, Default, Copy, Clone, Error)]
pub(crate) enum ErrorCode {
    UnknownServerError = -1,
    #[default]
    None = 0,
    UnsupportedVersion = 35,
    InvalidRequest = 42,
    // TODO: Add more from https://kafka.apache.org/protocol.html#protocol_error_codes
}

pub(crate) type Result<T> = result::Result<T, ErrorCode>;

impl From<ErrorCode> for Bytes {
    fn from(error_code: ErrorCode) -> Self {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_i16(error_code as i16);
        buf.freeze()
    }
}

impl From<io::Error> for ErrorCode {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::InvalidData => ErrorCode::InvalidRequest,
            _ => ErrorCode::UnknownServerError,
        }
    }
}

// TODO: Make this actually unsigned varint
pub(crate) type UnsignedVarint = u8;

#[derive(Default)]
pub(crate) struct CompactArray<Item> {
    pub(crate) length: UnsignedVarint,
    pub(crate) elements: Vec<Item>,
}

// TODO: Add null array case
#[allow(clippy::cast_possible_truncation)]
impl<T> From<Vec<T>> for CompactArray<T> {
    fn from(value: Vec<T>) -> Self {
        CompactArray {
            length: value.len() as UnsignedVarint,
            elements: value,
        }
    }
}

impl<Item> BufMutExt<CompactArray<Item>> for BytesMut
where
    bytes::BytesMut: BufMutExt<Item>,
{
    fn put_api_response_payload(&mut self, compact_array: &CompactArray<Item>) {
        self.put_u8(compact_array.length + 1);
        for x in &compact_array.elements {
            self.put_api_response_payload(x);
        }
    }
}

pub(crate) type Int32 = i32;

// TODO: Make this actually TagBuffer, see https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=120722234#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-Serialization
pub(crate) type TagBuffer = i8;

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
