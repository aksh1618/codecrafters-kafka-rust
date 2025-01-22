use crate::api::{ApiKind, KafkaBrokerApi};
use crate::buf::BufMutExt;
use crate::model::Result;
use crate::model::*;
use bytes::{BufMut, Bytes, BytesMut};
use strum::IntoEnumIterator as _;

// #[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ApiVersionsV4;

impl KafkaBrokerApi for ApiVersionsV4 {
    fn kind(&self) -> ApiKind {
        ApiKind::ApiVersions
    }

    fn api_version(&self) -> ApiVersion {
        4
    }

    fn handle_request(&self, request: RequestMessageV2) -> Result<Bytes> {
        create_response(&request)
    }
}

fn validate(request_message: &RequestMessageV2) -> Option<ErrorCode> {
    if ApiVersionsV4.api_version() != request_message.header.api_version {
        return Some(ErrorCode::UnsupportedVersion);
    }
    None
}

fn create_response(request: &RequestMessageV2) -> Result<Bytes> {
    if let Some(error_code) = validate(request) {
        return Err(error_code);
    }
    let apis = ApiKind::iter()
        .map(ApiVersionSpec::from)
        .collect::<Vec<_>>();
    let apis = ApiVersionsResponsePayload::from(apis);
    let mut buf = BytesMut::new();
    buf.put_api_response_payload(&apis);
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
        ApiVersionsResponsePayload {
            api_versions: CompactArray::from(api_versions),
            ..Default::default()
        }
    }
}

// TODO: Update to this once generic implementation for CompactArray is done
// impl<T: BufMut> BufMutExt<ApiVersionsResponsePayload> for T {
impl BufMutExt<ApiVersionsResponsePayload> for BytesMut {
    fn put_api_response_payload(&mut self, response_payload: &ApiVersionsResponsePayload) {
        self.put_i16(response_payload.error_code as i16);
        self.put_api_response_payload(&response_payload.api_versions);
        self.put_i32(response_payload.throttle_time_ms);
        self.put_i8(response_payload.tag_buffer);
    }
}

#[derive(Default)]
struct ApiVersionSpec {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl From<ApiKind> for ApiVersionSpec {
    fn from(api_kind: ApiKind) -> Self {
        let supported_versions = SupportedVersions::from(api_kind);
        ApiVersionSpec {
            api_key: api_kind.into(),
            min_version: supported_versions.min_version,
            max_version: supported_versions.max_version,
        }
    }
}

impl<T: BufMut> BufMutExt<ApiVersionSpec> for T {
    fn put_api_response_payload(&mut self, api_spec: &ApiVersionSpec) {
        self.put_i16(api_spec.api_key);
        self.put_i16(api_spec.min_version);
        self.put_i16(api_spec.max_version);
        let tag_buffer = TagBuffer::default();
        self.put_i8(tag_buffer);
    }
}
