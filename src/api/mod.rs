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
use bytes::Bytes;
use describe_topic_partitions::DescribeTopicPartitionsV0;
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

pub struct Response {
    pub message_size: i32,
    pub message: ResponsePayload,
}

pub struct RequestMessageV2 {
    pub header: RequestHeaderV2,
    pub payload: RequestPayload,
}

#[derive(Debug, Default)]
pub struct RequestHeaderV2 {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
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

pub fn handle_request(request_message_v2: RequestMessageV2) -> Bytes {
    let api_key = request_message_v2.header.api_key;
    let api_kind = match api_key.try_into() {
        Ok(api_kind) => api_kind,
        Err::<_, ErrorCode>(error_code) => return error_code.into(),
    };

    let apis = apis_for_kind(api_kind);
    assert!(
        !apis.is_empty(),
        "No apis for {api_kind} api, even though it's mapped for api key {api_key}"
    );

    let api_version = request_message_v2.header.api_version;
    let Some(api) = apis.iter().find(|api| api.api_version() == api_version) else {
        println!("Version {api_version} not supported for {api_kind} api");
        return ErrorCode::UnsupportedVersion.into();
    };

    match api.handle_request(request_message_v2) {
        Ok(api_response) => api_response,
        Err(error_code) => {
            println!("Error: {error_code}");
            error_code.into()
        }
    }
}
