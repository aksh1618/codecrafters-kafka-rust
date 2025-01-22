use crate::model::*;

use api_versions::ApiVersionsV4;
use bytes::Bytes;
use describe_topic_partitions::DescribeTopicPartitionsV0;
use strum::{Display, EnumIter, FromRepr};

pub(crate) mod api_versions;
pub(crate) mod describe_topic_partitions;

#[derive(Display, Debug, Clone, Copy, FromRepr, EnumIter)]
#[repr(i16)]
pub(crate) enum ApiKind {
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

fn apis_for_kind(api_kind: ApiKind) -> Vec<Box<dyn KafkaBrokerApi>> {
    match api_kind {
        ApiKind::ApiVersions => vec![Box::new(ApiVersionsV4)],
        ApiKind::DescribeTopicPartitions => vec![Box::new(DescribeTopicPartitionsV0)],
    }
}

impl From<ApiKind> for ApiKey {
    fn from(api_kind: ApiKind) -> Self {
        api_kind as i16
    }
}

impl TryFrom<ApiKey> for ApiKind {
    type Error = ErrorCode;

    fn try_from(api_key: ApiKey) -> Result<Self> {
        ApiKind::from_repr(api_key).ok_or(ErrorCode::InvalidRequest)
    }
}

// TODO: Should this be in api_versions.rs?
impl From<ApiKind> for SupportedVersions {
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
        SupportedVersions {
            min_version,
            max_version,
        }
    }
}

#[allow(
    dead_code,
    reason = "kind & api_key not being used currently, but need to keep kind reverse mapping in
    implementation and might be used in future"
)]
pub(crate) trait KafkaBrokerApi {
    fn kind(&self) -> ApiKind;
    fn api_version(&self) -> ApiVersion;
    fn api_key(&self) -> ApiKey {
        self.kind().into()
    }
    fn handle_request(&self, request: RequestMessageV2) -> Result<ResponsePayload>;
}

pub(crate) fn handle_request(request_message_v2: RequestMessageV2) -> Bytes {
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
