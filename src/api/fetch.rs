use bytes::BytesMut;
use encode_decode_derive::{Decode, Encode};

use super::{RequestMessageV2, ResponsePayload};
use crate::api::{ApiKind, KafkaBrokerApi};
use crate::buf::{BufExt as _, BufMutExt as _};
use crate::log::Record;
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
        dbg!(&request);
        if let Some(error_code) = validate(&request) {
            return Err(error_code);
        }
        let request = request.payload.get_decoded();
        dbg!(&request);

        let response = create_response(request);
        let mut buf = BytesMut::new();
        buf.put_encoded(&response);
        Ok(buf.into())
    }
}

fn validate(request_message: &RequestMessageV2) -> Option<ErrorCode> {
    if FetchV16.api_version() != request_message.header.api_version {
        return Some(ErrorCode::UnsupportedVersion);
    }
    None
}

fn create_response(_request: FetchRequest) -> FetchResponse {
    FetchResponse::default()
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/FetchRequest.json
#[derive(Debug, Default, Encode, Decode)]
pub struct FetchRequest {
    // cluster_id: CompactString, // tagged
    // replica_state: ReplicaState, // tagged
    max_wait_ms: Int32,
    min_bytes: Int32,
    max_bytes: Int32, // default: '0x7fffffff'
    isolation_level: Int8,
    session_id: Int32,
    session_epoch: Int32,
    topics: CompactArray<FetchTopic>,
    forgotten_topics_data: CompactArray<ForgottenTopic>,
    rack_id: CompactString,
    tag_buffer: TagBuffer,
}

// #[derive(Debug, Default, Encode, Decode)]
// pub struct ReplicaState {
//     replica_id: Int32,
//     replica_epoch: Int64,
//     tag_buffer: TagBuffer,
// }

#[derive(Debug, Default, Encode, Decode)]
pub struct FetchTopic {
    topic_id: Uuid,
    partitions: CompactArray<FetchPartition>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct FetchPartition {
    partition: Int32,
    current_leader_epoch: Int32,
    fetch_offset: Int64,
    last_fetched_epoch: Int32,
    log_start_offset: Int64,
    partition_max_bytes: Int32,
    // replica_directory_id: Uuid, // version >= 17
    tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct ForgottenTopic {
    topic_id: Uuid,
    partitions: CompactArray<Int32>,
    tag_buffer: TagBuffer,
}

impl FetchRequest {
    pub fn new(topics: Vec<Uuid>) -> Self {
        Self {
            topics: CompactArray::from(
                topics
                    .into_iter()
                    .map(|topic_id| FetchTopic {
                        topic_id,
                        ..Default::default()
                    })
                    .collect::<Vec<_>>(),
            ),
            ..Default::default()
        }
    }
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/FetchResponse.json
#[derive(Debug, Default, Encode, Decode)]
pub struct FetchResponse {
    pub throttle_time_ms: Int32,
    pub error_code: ErrorCode,
    pub session_id: Int32,
    pub responses: CompactArray<FetchableTopicResponse>,
    // pub node_endpoints: CompactArray<NodeEndpoint>, // tagged
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct FetchableTopicResponse {
    pub topic_id: Uuid,
    pub partitions: CompactArray<PartitionData>,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct PartitionData {
    pub partition_index: Int32,
    pub error_code: ErrorCode,
    pub high_watermark: Int64,
    pub last_stable_offset: Int64,
    pub log_start_offset: Int64,
    // pub diverging_epoch: EpochEndOffset, // tagged
    // pub current_leader: LeaderIdAndEpoch,
    // pub snapshot_id: SnapshotId,
    pub aborted_transactions: CompactArray<AbortedTransaction>,
    pub preferred_read_replica: Int32,
    pub records: CompactArray<Record>, // TODO: Verify this correctly represents `COMPACT_RECORDS`
    pub tag_buffer: TagBuffer,
}

// #[derive(Debug, Default, Encode, Decode)]
// pub struct EpochEndOffset {
//     pub epoch: Int32,
//     pub end_offset: Int64,
// }

// #[derive(Debug, Default, Encode, Decode)]
// pub struct LeaderIdAndEpoch {
//     pub leader_id: Int32,
//     pub leader_epoch: Int32,
// }

// #[derive(Debug, Default, Encode, Decode)]
// pub struct SnapshotId {
//     pub end_offset: Int64,
//     pub epoch: Int32,
// }

#[derive(Debug, Default, Encode, Decode)]
pub struct AbortedTransaction {
    pub producer_id: Int64,
    pub first_offset: Int64,
}

// #[derive(Debug, Default, Encode, Decode)]
// pub struct RecordSet {
//     pub records: VarintArray<Record>,
// }

// #[derive(Debug, Default, Encode, Decode)]
// pub struct NodeEndpoint {
//     pub node_id: Int32,
//     pub host: CompactString,
//     pub port: Int32,
//     pub rack: CompactNullableString,
// }

#[allow(
    clippy::restriction,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::server::tests::perform_request;
    use anyhow::Result;

    #[test]
    fn test_fetch_v16_no_topics() -> Result<()> {
        // Given
        const TOPICS: &[Uuid] = &[];

        // When
        let fetch_response = perform_fetch_request(TOPICS.to_vec())?;

        // Then
        dbg!(&fetch_response);
        assert_eq!(fetch_response.throttle_time_ms, 0);
        assert_eq!(fetch_response.session_id, 0);
        assert_eq!(fetch_response.responses.length, 0);
        Ok(())
    }

    fn perform_fetch_request(topics: Vec<Uuid>) -> Result<FetchResponse> {
        let fetch_request = FetchRequest::new(topics);
        let response = perform_request(ApiKind::Fetch, 16, &fetch_request)?;
        let fetch_response = response.message.payload().get_decoded();
        Ok(fetch_response)
    }
}
