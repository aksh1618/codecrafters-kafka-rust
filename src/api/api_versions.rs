use super::apis_for_kind;
use super::{RequestMessageV2, ResponsePayload};
use crate::api::{ApiKind, KafkaBrokerApi};
use crate::buf::BufMutExt as _;
use crate::model::Result;
use crate::model::*;
use bytes::BytesMut;
use encode_decode_derive::Encode;
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

// TODO: Refactor these tests to use server::tests::perform_request
#[allow(
    clippy::restriction,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
#[cfg(test)]
mod test {
    use crate::server::{tests::ensure_server_running, ADDR};
    use bytes::{Buf as _, BufMut as _};
    use std::{
        io::{Read as _, Result, Write as _},
        net::{self, TcpStream},
        thread,
        time::Instant,
    };

    #[test]
    fn test_apiversions_v4_request_response() -> Result<()> {
        #[expect(clippy::unreadable_literal)]
        const CORRELATION_ID: i32 = 1857043921i32;
        ensure_server_running();
        let mut stream = net::TcpStream::connect(ADDR)?;
        test_apiversions_v4_request_response_for_correlation_id(&mut stream, CORRELATION_ID)
    }

    fn test_apiversions_v4_request_response_for_correlation_id(
        stream: &mut TcpStream,
        correlation_id: i32,
    ) -> Result<()> {
        let mut buf = vec![];
        buf.put_i16(18i16);
        buf.put_i16(4i16);
        buf.put_i32(correlation_id);
        buf.put_i16(-1i16);
        buf.put_i8(0i8);
        stream.write_all((buf.len() as i32).to_be_bytes().as_slice())?;
        stream.write_all(&buf)?;
        stream.flush()?;
        println!("Wrote and flushed all request bytes for {correlation_id}");

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf)?;
        let mut bytes = buf.as_slice();
        let message_size = bytes.get_i32();
        println!("Received response message size: {message_size}");
        let remaining_bytes = message_size as usize;
        let mut buf = vec![0u8; remaining_bytes];
        stream.read_exact(&mut buf)?;
        let mut bytes = buf.as_slice();
        let res_correlation_id = bytes.get_i32();
        assert_eq!(res_correlation_id, correlation_id);
        let error_code = bytes.get_i16();
        assert_eq!(error_code, 0i16);
        let num_api_keys = bytes.get_u8() - 1;
        assert_eq!(num_api_keys, 3u8);
        let apiversions_api_key = bytes.get_i16();
        assert_eq!(apiversions_api_key, 18i16);
        let apiversions_min_version = bytes.get_i16();
        assert_eq!(apiversions_min_version, 4i16);
        let apiversions_max_version = bytes.get_i16();
        assert_eq!(apiversions_max_version, 4i16);
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        let describe_topic_partitions_api_key = bytes.get_i16();
        assert_eq!(describe_topic_partitions_api_key, 75i16);
        let describe_topic_partitions_min_version = bytes.get_i16();
        assert_eq!(describe_topic_partitions_min_version, 0i16);
        let describe_topic_partitions_max_version = bytes.get_i16();
        assert_eq!(describe_topic_partitions_max_version, 0i16);
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        let fetch_api_key = bytes.get_i16();
        assert_eq!(fetch_api_key, 1i16);
        let fetch_min_version = bytes.get_i16();
        assert_eq!(fetch_min_version, 16i16);
        let fetch_max_version = bytes.get_i16();
        assert_eq!(fetch_max_version, 16i16);
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        let throttle_time_ms = bytes.get_i32();
        assert_eq!(throttle_time_ms, 0i32);
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        assert_eq!(bytes.len(), 0);
        println!(
            "Read {} response bytes for {res_correlation_id}",
            message_size + 4
        );
        Ok(())
    }

    #[test]
    #[expect(clippy::unreadable_literal)]
    fn test_multiple_serial_apiversions_v4_request_response() -> Result<()> {
        const CORRELATION_ID_1: i32 = 1956054920i32;
        const CORRELATION_ID_2: i32 = 205506591i32;
        const CORRELATION_ID_3: i32 = 215707621i32;
        ensure_server_running();
        let mut stream = net::TcpStream::connect(ADDR)?;
        test_apiversions_v4_request_response_for_correlation_id(&mut stream, CORRELATION_ID_1)?;
        test_apiversions_v4_request_response_for_correlation_id(&mut stream, CORRELATION_ID_2)?;
        test_apiversions_v4_request_response_for_correlation_id(&mut stream, CORRELATION_ID_3)?;
        Ok(())
    }

    // TODO: Improve this test, can be flaky
    #[test]
    #[expect(clippy::unreadable_literal)]
    fn test_multiple_parallel_apiversions_v4_request_response() {
        const CORRELATION_IDS: [i32; 5] = [
            1956054920i32,
            205506591i32,
            215707621i32,
            1857043921i32,
            1857043922i32,
        ];
        let repetitions = CORRELATION_IDS.len();
        ensure_server_running();
        let mut threads = Vec::new();
        let start_time = Instant::now();
        for correlation_id in CORRELATION_IDS {
            let _ = net::TcpStream::connect(ADDR).map(|mut stream| {
                test_apiversions_v4_request_response_for_correlation_id(&mut stream, correlation_id)
            });
        }
        let time_taken_ser = start_time.elapsed();
        println!("Time taken for 3 serial request: {time_taken_ser:?}");
        let start_time = Instant::now();
        for correlation_id in CORRELATION_IDS {
            for _ in 0..repetitions {
                threads.push(thread::spawn(move || {
                    let _ = net::TcpStream::connect(ADDR).map(|mut stream| {
                        test_apiversions_v4_request_response_for_correlation_id(
                            &mut stream,
                            correlation_id,
                        )
                    });
                    // thread::sleep(std::time::Duration::from_millis(100));
                }));
            }
        }
        for thread in threads {
            thread.join().expect("should be able to join thread");
        }
        let time_taken_par = start_time.elapsed();
        assert!(time_taken_par < time_taken_ser * repetitions as u32);
        println!("Time taken for 3 parallel requests: {time_taken_par:?}");
    }
}
