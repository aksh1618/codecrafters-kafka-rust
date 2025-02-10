#![allow(
    clippy::module_name_repetitions,
    reason = "We're going to use file-per-api structuring, with all versions of the api in the
    corresponding file. So the struct names are going to be of the form ApiNameV0."
)]
#![allow(
    dead_code,
    reason = "Going to evolve this as we go, so allowing some dead code for now"
)]

use crate::{
    buf::{BufExt as _, BufMutExt as _},
    model::*,
};

use api_versions::ApiVersionsV4;
use bytes::{Buf, Bytes, BytesMut};
use describe_topic_partitions::DescribeTopicPartitionsV0;
use encode_decode_derive::{Decode, Encode};
use strum::{Display, EnumIter, FromRepr};

pub mod api_versions;
pub mod describe_topic_partitions;

#[derive(Display, Debug, Clone, Copy, FromRepr, EnumIter)]
#[repr(i16)]
pub enum ApiKind {
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

pub fn apis_for_kind(api_kind: ApiKind) -> Vec<Box<dyn KafkaBrokerApi>> {
    match api_kind {
        ApiKind::ApiVersions => vec![Box::new(ApiVersionsV4)],
        ApiKind::DescribeTopicPartitions => vec![Box::new(DescribeTopicPartitionsV0)],
    }
}

impl From<ApiKind> for ApiKey {
    fn from(api_kind: ApiKind) -> Self {
        api_kind as Self
    }
}

impl TryFrom<ApiKey> for ApiKind {
    type Error = ErrorCode;

    fn try_from(api_key: ApiKey) -> Result<Self> {
        Self::from_repr(api_key).ok_or(ErrorCode::InvalidRequest)
    }
}

#[derive(Debug)]
pub struct RequestV2 {
    pub message_size: i32,
    pub message: RequestMessageV2,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct RequestMessageV2 {
    pub header: RequestHeaderV2,
    pub payload: RequestPayload,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct RequestHeaderV2 {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: NullableString,
    pub tag_buffer: TagBuffer,
}

impl RequestHeaderV2 {
    // TODO: convert this into a trait, can this be added to Encode trait?
    fn encoded_size(&self) -> usize {
        size_of_val(&self.api_key)
            + size_of_val(&self.api_version)
            + size_of_val(&self.correlation_id)
            + self.client_id.encoded_size()
            + size_of_val(&self.tag_buffer)
    }
}

impl RequestV2 {
    pub fn new(
        api_kind: ApiKind,
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: &str,
        payload: RequestPayload,
    ) -> Self {
        let header = RequestHeaderV2 {
            api_key: api_kind.into(),
            api_version,
            correlation_id,
            client_id: NullableString::from(client_id),
            ..Default::default()
        };
        let message_size = i32::try_from(header.encoded_size() + payload.len())
            .expect("message size shouldn't be large enough to wrap around when converted to i32");
        Self {
            message_size,
            message: RequestMessageV2 { header, payload },
        }
    }

    pub fn into_bytes(self) -> Bytes {
        let message_size = self.message_size;
        let header = self.message.header;
        let payload = self.message.payload;
        let request_size = size_of_val(&message_size)
            + usize::try_from(message_size).expect("message size should be non-negative");
        let mut header_buf = BytesMut::with_capacity(header.encoded_size());
        header_buf.put_encoded(&header);
        (message_size.to_be_bytes().as_slice())
            .chain(header_buf.freeze())
            .chain(payload)
            .copy_to_bytes(request_size)
    }
}

#[derive(Debug, Decode)]
pub struct ResponseV0 {
    pub message_size: i32,
    pub message: ResponseMessageV0,
}

#[derive(Debug, Default, Decode)]
pub struct ResponseMessageV0 {
    pub header: ResponseHeaderV0,
    pub payload: ResponsePayload,
}

#[derive(Debug, Default, Decode)]
pub struct ResponseHeaderV0 {
    pub correlation_id: CorrelationId,
}

#[derive(Debug)]
pub struct Response {
    pub message_size: i32,
    pub message: ResponseMessage,
}

#[derive(Debug, Default, Decode)]
pub struct ResponseMessageV1 {
    pub header: ResponseHeaderV1,
    pub payload: ResponsePayload,
}

#[derive(Debug, Default, Decode)]
pub struct ResponseHeaderV1 {
    pub correlation_id: CorrelationId,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug)]
pub enum ResponseMessage {
    V0(ResponseMessageV0),
    V1(ResponseMessageV1),
}

impl ResponseMessage {
    pub fn new(api_kind: ApiKind, correlation_id: CorrelationId, payload: ResponsePayload) -> Self {
        if matches!(api_kind, ApiKind::ApiVersions) {
            Self::new_v0(correlation_id, payload)
        } else {
            Self::new_v1(correlation_id, payload)
        }
    }

    pub const fn new_v0(correlation_id: CorrelationId, payload: ResponsePayload) -> Self {
        Self::V0(ResponseMessageV0 {
            header: ResponseHeaderV0 { correlation_id },
            payload,
        })
    }

    pub fn new_v1(correlation_id: CorrelationId, payload: ResponsePayload) -> Self {
        let tag_buffer = TagBuffer::default();
        Self::V1(ResponseMessageV1 {
            header: ResponseHeaderV1 {
                correlation_id,
                tag_buffer,
            },
            payload,
        })
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::V0(response) => {
                size_of_val(&response.header.correlation_id) + response.payload.len()
            }
            Self::V1(response) => {
                size_of_val(&response.header.correlation_id)
                    + size_of_val(&response.header.tag_buffer)
                    + response.payload.len()
            }
        }
    }

    pub fn decode<B: Buf + ?Sized>(api_kind: ApiKind, mut buf: &mut B) -> Self {
        if matches!(api_kind, ApiKind::ApiVersions) {
            Self::V0(buf.get_decoded())
        } else {
            Self::V1(buf.get_decoded())
        }
    }

    pub const fn correlation_id(&self) -> CorrelationId {
        match self {
            Self::V0(response) => response.header.correlation_id,
            Self::V1(response) => response.header.correlation_id,
        }
    }

    pub fn payload(self) -> ResponsePayload {
        match self {
            Self::V0(response) => response.payload,
            Self::V1(response) => response.payload,
        }
    }
}

impl Response {
    pub fn new(message: ResponseMessage) -> Self {
        let message_size = i32::try_from(message.encoded_size())
            .expect("message size shouldn't be large enough to wrap around when converted to i32");
        Self {
            message_size,
            message,
        }
    }

    pub fn into_bytes(self) -> Bytes {
        match self.message {
            ResponseMessage::V0(response) => {
                let message_size = self.message_size;
                let correlation_id = response.header.correlation_id;
                let payload = response.payload;
                let response_size = size_of_val(&message_size)
                    + usize::try_from(message_size).expect("message size should be non-negative");
                (message_size.to_be_bytes().as_slice())
                    .chain(correlation_id.to_be_bytes().as_slice())
                    .chain(payload)
                    .copy_to_bytes(response_size)
            }
            ResponseMessage::V1(response) => {
                let message_size = self.message_size;
                let correlation_id = response.header.correlation_id;
                let tag_buffer = response.header.tag_buffer;
                let payload = response.payload;
                let response_size = size_of_val(&message_size)
                    + size_of_val(&correlation_id)
                    + size_of_val(&tag_buffer)
                    + payload.len();
                (message_size.to_be_bytes().as_slice())
                    .chain(correlation_id.to_be_bytes().as_slice())
                    .chain(tag_buffer.to_be_bytes().as_slice())
                    .chain(payload)
                    .copy_to_bytes(response_size)
            }
        }
    }
}

// TODO: Is this beneficial? Should we use newtype instead of alias?
pub type RequestPayload = Bytes;
pub type ResponsePayload = Bytes;
// pub type ResponsePayload<T: BufMut> = T;
// pub struct ResponsePayload<T: BufMut>(T);
// pub struct RequestPayload(Bytes);
// impl<T: buf::Encode> From<T> for RequestPayload {
//     fn from(request: T) -> Self {
//         let mut buf = vec![];
//         buf.put_encoded(&request);
//         Self(buf.into())
//     }
// }

#[expect(
    dead_code,
    reason = "kind & api_key not being used currently, but need to keep kind reverse mapping in
    implementation and might be used in future"
)]
pub trait KafkaBrokerApi {
    fn kind(&self) -> ApiKind;
    fn api_version(&self) -> ApiVersion;
    fn api_key(&self) -> ApiKey {
        self.kind().into()
    }
    fn handle_request(&self, request: RequestMessageV2) -> Result<ResponsePayload>;
}

pub fn handle_request(request_message_v2: RequestMessageV2) -> Response {
    let correlation_id = request_message_v2.header.correlation_id;
    let api_key = request_message_v2.header.api_key;
    let api_kind = match api_key.try_into() {
        Ok(api_kind) => api_kind,
        Err::<_, ErrorCode>(error_code) => {
            return Response::new(ResponseMessage::new_v0(correlation_id, error_code.into()));
        }
    };

    let apis = apis_for_kind(api_kind);
    assert!(
        !apis.is_empty(),
        "No apis for {api_kind} api, even though it's mapped for api key {api_key}"
    );

    let api_version = request_message_v2.header.api_version;
    let Some(api) = apis.iter().find(|api| api.api_version() == api_version) else {
        println!("Version {api_version} not supported for {api_kind} api");
        return Response::new(ResponseMessage::new(
            api_kind,
            correlation_id,
            ErrorCode::UnsupportedVersion.into(),
        ));
    };

    let payload = match api.handle_request(request_message_v2) {
        Ok(api_response) => api_response,
        Err(error_code) => {
            println!("Error: {error_code}");
            error_code.into()
        }
    };

    Response::new(ResponseMessage::new(api_kind, correlation_id, payload))
}
