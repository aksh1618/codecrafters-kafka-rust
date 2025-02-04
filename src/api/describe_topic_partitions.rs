use super::{RequestMessageV2, ResponsePayload};
use crate::api::{ApiKind, KafkaBrokerApi};
use crate::model::*;

// #[derive(Debug, Clone, Copy, Default)]
pub struct DescribeTopicPartitionsV0;

impl KafkaBrokerApi for DescribeTopicPartitionsV0 {
    fn kind(&self) -> ApiKind {
        ApiKind::DescribeTopicPartitions
    }

    fn api_version(&self) -> ApiVersion {
        0
    }

    fn handle_request(&self, request: RequestMessageV2) -> Result<ResponsePayload> {
        create_response(&request)
    }
}

fn validate(request_message: &RequestMessageV2) -> Option<ErrorCode> {
    if DescribeTopicPartitionsV0.api_version() != request_message.header.api_version {
        return Some(ErrorCode::UnsupportedVersion);
    }
    None
}

fn create_response(request: &RequestMessageV2) -> Result<ResponsePayload> {
    if let Some(error_code) = validate(request) {
        return Err(error_code);
    }
    todo!()
}
