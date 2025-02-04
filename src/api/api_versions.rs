use super::apis_for_kind;
use super::{RequestMessageV2, ResponsePayload};
use crate::api::{ApiKind, KafkaBrokerApi};
use crate::buf::BufMutExt as _;
use crate::model::Result;
use crate::model::*;
use bytes::BytesMut;
use encode_derive::Encode;
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

#[derive(Debug, Default, Encode)]
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

#[derive(Debug, Default, Encode)]
struct ApiVersionSpec {
    api_key: Int16,
    min_version: Int16,
    max_version: Int16,
    tag_buffer: TagBuffer,
}

impl From<ApiKind> for ApiVersionSpec {
    fn from(api_kind: ApiKind) -> Self {
        let apis = apis_for_kind(api_kind);
        let min_version = apis
            .iter()
            .map(|api| api.api_version())
            .min()
            .unwrap_or_else(|| panic!("Unsupported api kind {api_kind}"));
        let max_version = apis
            .iter()
            .map(|api| api.api_version())
            .max()
            .unwrap_or_else(|| panic!("Unsupported api kind {api_kind}"));
        Self {
            api_key: api_kind.into(),
            min_version,
            max_version,
            ..Default::default()
        }
    }
}
