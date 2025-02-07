#![allow(
    clippy::module_name_repetitions,
    reason = "We're going to use file-per-api structuring, with all versions of the api in the
    corresponding file. So the struct names are going to be of the form ApiNameV0."
)]
#![allow(
    dead_code,
    reason = "Going to evolve this as we go, so allowing some dead code for now"
)]

use crate::model::*;

use api_versions::ApiVersionsV4;
use bytes::{Buf as _, Bytes};
use describe_topic_partitions::DescribeTopicPartitionsV0;
use encode_decode_derive::Decode;
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

pub struct RequestV2 {
    pub size: i32,
    pub message: RequestMessageV2,
}

#[derive(Debug, Default, Decode)]
pub struct RequestMessageV2 {
    pub header: RequestHeaderV2,
    pub payload: RequestPayload,
}

#[derive(Debug, Default, Decode)]
pub struct RequestHeaderV2 {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub client_id: NullableString,
    pub tag_buffer: TagBuffer,
}

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

pub struct ResponseV1 {
    pub message_size: i32,
    pub message: ResponseMessageV1,
}

pub enum Response {
    V0(ResponseV0),
    V1(ResponseV1),
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

impl Response {
    pub fn new(api_kind: ApiKind, correlation_id: CorrelationId, payload: ResponsePayload) -> Self {
        match api_kind {
            ApiKind::ApiVersions => Response::new_v0(correlation_id, payload),
            _ => Response::new_v1(correlation_id, payload),
        }
    }

    pub fn new_v0(correlation_id: CorrelationId, payload: ResponsePayload) -> Self {
        let message_size = i32::try_from(size_of_val(&correlation_id) + payload.len())
            .expect("message size shouldn't be large enough to wrap around when converted to i32");
        Response::V0(ResponseV0 {
            message_size,
            message: ResponseMessageV0 {
                header: ResponseHeaderV0 { correlation_id },
                payload,
            },
        })
    }

    pub fn new_v1(correlation_id: CorrelationId, payload: ResponsePayload) -> Self {
        let tag_buffer = TagBuffer::default();
        let message_size =
            i32::try_from(size_of_val(&correlation_id) + size_of_val(&tag_buffer) + payload.len())
                .expect(
                    "message size shouldn't be large enough to wrap around when converted to i32",
                );
        Response::V1(ResponseV1 {
            message_size,
            message: ResponseMessageV1 {
                header: ResponseHeaderV1 {
                    correlation_id,
                    tag_buffer,
                },
                payload,
            },
        })
    }

    pub fn message_size(&self) -> i32 {
        match self {
            Response::V0(response) => response.message_size,
            Response::V1(response) => response.message_size,
        }
    }

    pub fn correlation_id(&self) -> CorrelationId {
        match self {
            Response::V0(response) => response.message.header.correlation_id,
            Response::V1(response) => response.message.header.correlation_id,
        }
    }

    pub fn payload(&self) -> &ResponsePayload {
        match self {
            Response::V0(response) => &response.message.payload,
            Response::V1(response) => &response.message.payload,
        }
    }

    pub fn to_bytes(self) -> Bytes {
        match self {
            Response::V0(response) => {
                let message_size = response.message_size;
                let correlation_id = response.message.header.correlation_id;
                let payload = response.message.payload;
                let response_size = size_of_val(&message_size) + message_size as usize;
                let response = (message_size.to_be_bytes().as_slice())
                    .chain(correlation_id.to_be_bytes().as_slice())
                    .chain(payload)
                    .copy_to_bytes(response_size);
                response
            }
            Response::V1(response) => {
                let message_size = response.message_size;
                let correlation_id = response.message.header.correlation_id;
                let tag_buffer = response.message.header.tag_buffer;
                let payload = response.message.payload;
                let response_size = size_of_val(&message_size)
                    + size_of_val(&correlation_id)
                    + size_of_val(&tag_buffer)
                    + payload.len();
                let response = (message_size.to_be_bytes().as_slice())
                    .chain(correlation_id.to_be_bytes().as_slice())
                    .chain(tag_buffer.to_be_bytes().as_slice())
                    .chain(payload)
                    .copy_to_bytes(response_size);
                response
            }
        }
    }
}

// TODO: Is this beneficial? Should we use newtype instead of alias?
pub type RequestPayload = Bytes;
pub type ResponsePayload = Bytes;
// pub type ResponsePayload<T: BufMut> = T;
// pub struct ResponsePayload<T: BufMut>(T);

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
            return Response::new_v0(correlation_id, error_code.into());
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
        return Response::new(
            api_kind,
            correlation_id,
            ErrorCode::UnsupportedVersion.into(),
        );
    };

    let payload = match api.handle_request(request_message_v2) {
        Ok(api_response) => api_response,
        Err(error_code) => {
            println!("Error: {error_code}");
            error_code.into()
        }
    };

    Response::new(api_kind, correlation_id, payload)
}
