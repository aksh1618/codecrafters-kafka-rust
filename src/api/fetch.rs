use super::{RequestMessageV2, ResponsePayload};
use crate::api::{ApiKind, KafkaBrokerApi};
use crate::model::*;

// #[derive(Debug, Clone, Copy, Default)]
pub struct FetchV16;

impl KafkaBrokerApi for FetchV16 {
    fn kind(&self) -> ApiKind {
        ApiKind::Fetch
    }

    fn api_version(&self) -> ApiVersion {
        16
    }

    fn handle_request(&self, mut request: RequestMessageV2) -> Result<ResponsePayload> {
        create_response(&mut request)
    }
}

fn validate(request_message: &RequestMessageV2) -> Option<ErrorCode> {
    if FetchV16.api_version() != request_message.header.api_version {
        return Some(ErrorCode::UnsupportedVersion);
    }
    None
}

fn create_response(request: &mut RequestMessageV2) -> Result<ResponsePayload> {
    // dbg!(&request);
    if let Some(error_code) = validate(request) {
        return Err(error_code);
    }
    todo!()
}
