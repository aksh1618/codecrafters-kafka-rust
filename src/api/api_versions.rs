use crate::api::{ApiKind, KafkaBrokerApi};
use crate::buf::{BufMutExt as _, Encode};
use crate::model::Result;
use crate::model::*;
use bytes::{BufMut, BytesMut};
use strum::IntoEnumIterator as _;

// #[derive(Debug, Clone, Copy, Default)]
pub struct ApiVersionsV4;

impl KafkaBrokerApi for ApiVersionsV4 {
    fn kind(&self) -> ApiKind {
        ApiKind::ApiVersions
    }

    fn api_version(&self) -> ApiVersion {
        4
    }

    fn handle_request(&self, request: RequestMessageV2) -> Result<ResponsePayload> {
        create_response(&request)
    }
}

fn validate(request_message: &RequestMessageV2) -> Option<ErrorCode> {
    if ApiVersionsV4.api_version() != request_message.header.api_version {
        return Some(ErrorCode::UnsupportedVersion);
    }
    None
}

fn create_response(request: &RequestMessageV2) -> Result<ResponsePayload> {
    if let Some(error_code) = validate(request) {
        return Err(error_code);
    }
    let apis = ApiKind::iter()
        .map(ApiVersionSpec::from)
        .collect::<Vec<_>>();
    let apis = ApiVersionsResponsePayload::from(apis);
    let mut buf = BytesMut::new();
    buf.put_encoded(&apis);
    Ok(buf.into())
}

#[derive(Default)]
struct ApiVersionsResponsePayload {
    error_code: ErrorCode,
    api_versions: CompactArray<ApiVersionSpec>,
    throttle_time_ms: Int32,
    tag_buffer: TagBuffer,
}

impl ApiVersionsResponsePayload {
    fn from(api_versions: Vec<ApiVersionSpec>) -> Self {
        Self {
            api_versions: CompactArray::from(api_versions),
            ..Default::default()
        }
    }
}

impl Encode for ApiVersionsResponsePayload {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        buf.put_i16(self.error_code as i16);
        buf.put_encoded(&self.api_versions);
        buf.put_i32(self.throttle_time_ms);
        buf.put_i8(self.tag_buffer);
    }
}

#[derive(Default)]
struct ApiVersionSpec {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    tag_buffer: TagBuffer,
}

impl From<ApiKind> for ApiVersionSpec {
    fn from(api_kind: ApiKind) -> Self {
        let supported_versions = SupportedVersions::from(api_kind);
        Self {
            api_key: api_kind.into(),
            min_version: supported_versions.min_version,
            max_version: supported_versions.max_version,
            ..Default::default()
        }
    }
}

impl Encode for ApiVersionSpec {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        buf.put_i16(self.api_key);
        buf.put_i16(self.min_version);
        buf.put_i16(self.max_version);
        buf.put_encoded(&self.tag_buffer);
    }
}
