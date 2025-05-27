use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use encode_decode_derive::{Decode, Encode};

use super::{RequestMessageV2, ResponsePayload};
use crate::api::{ApiKind, KafkaBrokerApi};
use crate::buf::{BufExt as _, BufMutExt as _};
use crate::log::{
    read_metadata_records, read_raw_records, CompactRecords, MetadataRecordMessage,
    CLUSTER_METADATA_PATH,
};
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
        if let Some(error_code) = validate(&request) {
            return Err(error_code);
        }
        let request = request.payload.get_decoded();

        let metadata_records = read_metadata_records().map_err(|e| {
            eprintln!("Failed reading cluster metadata file from {CLUSTER_METADATA_PATH}: {e}");
            ErrorCode::UnknownServerError
        })?;
        let mut topic_names = HashMap::new();
        for record in metadata_records {
            // Soft TODO: if-let-chains?!
            if let MetadataRecordMessage::TopicRecord(topic_record) = record {
                topic_names.insert(topic_record.topic_id, topic_record.name.value);
            }
        }

        let response = create_response(request, &topic_names);
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

fn create_response(request: FetchRequest, topic_names: &HashMap<Uuid, Bytes>) -> FetchResponse {
    let responses = request
        .topics
        .elements
        .into_iter()
        .map(|topic| FetchableTopicResponse {
            topic_id: topic.topic_id,
            partitions: get_partitions_response_for_topic(&topic, topic_names.get(&topic.topic_id)),
            ..Default::default()
        })
        .collect::<Vec<_>>()
        .into();
    FetchResponse {
        responses,
        ..Default::default()
    }
}

fn get_partitions_response_for_topic(
    topic: &FetchTopic,
    topic_name: Option<&Bytes>,
) -> CompactArray<PartitionData> {
    let Some(topic_name) = topic_name else {
        let vec = vec![PartitionData {
            error_code: ErrorCode::UnknownTopicId,
            ..Default::default()
        }];
        return vec.into();
    };

    topic
        .partitions
        .elements
        .iter()
        .map(|partition| get_partition_response_for_topic_and_partition(topic_name, partition))
        .collect::<Vec<_>>()
        .into()
}

fn get_partition_response_for_topic_and_partition(
    topic_name: &Bytes,
    partition: &FetchPartition,
) -> PartitionData {
    let record_batches = match read_raw_records(topic_name, partition.partition) {
        Err(e) => {
            eprintln!("Failed reading partition log file: {e}");
            return PartitionData {
                error_code: ErrorCode::UnknownServerError,
                ..Default::default()
            };
        }
        Ok(record_batches) => record_batches,
    };
    let records = record_batches.into();
    PartitionData {
        records,
        ..Default::default()
    }
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
                        partitions: vec![FetchPartition::default()].into(),
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
    // The exact type of this was tough to find, got a hint from
    // https://rohithsankepally.github.io/Kafka-Storage-Internals/
    // where he uses kafka's DumpLogSegments tool to dump contents
    // of the log file. In hindsight I could've also tried writing
    // a log file by producing a message locally and then trying to
    // parse it into RecordBatch, but for now running & analyzing
    // the output of `codecrafters test` with debug mode enabled
    // had to suffice!
    pub records: CompactRecords,
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
    use crate::{
        log::{
            test::{
                write_test_data_to_cluster_metadata_log_file, write_test_data_to_partition_log_file,
            },
            RecordBatch,
        },
        server::tests::perform_request,
    };
    use anyhow::Result;

    #[test]
    fn test_fetch_v16_no_topics() -> Result<()> {
        // Given
        const TOPICS: &[Uuid] = &[];

        // When
        let fetch_response = perform_fetch_request(TOPICS.to_vec())?;

        // Then
        assert_eq!(fetch_response.throttle_time_ms, 0);
        assert_eq!(fetch_response.session_id, 0);
        assert_eq!(fetch_response.responses.elements.len(), 0);
        Ok(())
    }

    #[test]
    fn test_fetch_v16_unknown_topic() -> Result<()> {
        // Given
        const UNKNOWN_TOPIC_ID: Uuid = Uuid::max();
        const TOPICS: &[Uuid] = &[UNKNOWN_TOPIC_ID];

        // When
        let fetch_response = perform_fetch_request(TOPICS.to_vec())?;

        // Then
        assert_eq!(fetch_response.throttle_time_ms, 0);
        assert_eq!(fetch_response.session_id, 0);
        assert_eq!(fetch_response.responses.elements.len(), 1);
        let response = &fetch_response.responses.elements[0];
        assert_eq!(response.topic_id, UNKNOWN_TOPIC_ID);
        assert_eq!(response.partitions.elements.len(), 1);
        let partition_data = &response.partitions.elements[0];
        assert_eq!(partition_data.error_code, ErrorCode::UnknownTopicId);
        Ok(())
    }

    #[test]
    fn test_fetch_v16_empty_topic() -> Result<()> {
        // Given
        let record_batches = write_test_data_to_cluster_metadata_log_file()?;
        let (empty_topic_id, empty_topic_name) = select_topic_from_metadata(record_batches);
        let partition_data: &[u8] = b"";
        write_test_data_to_partition_log_file(&empty_topic_name, 0, &Bytes::from(partition_data))?;
        let topics = &[empty_topic_id];

        // When
        let fetch_response = perform_fetch_request(topics.to_vec())?;

        // Then
        assert_eq!(fetch_response.throttle_time_ms, 0);
        assert_eq!(fetch_response.session_id, 0);
        assert_eq!(fetch_response.responses.elements.len(), 1);
        let response = &fetch_response.responses.elements[0];
        assert_eq!(response.topic_id, empty_topic_id);
        assert_eq!(response.partitions.elements.len(), 1);
        let partition_data = &response.partitions.elements[0];
        assert_eq!(partition_data.error_code, ErrorCode::None);
        Ok(())
    }

    #[test]
    fn test_fetch_v16_topic_with_single_record() -> Result<()> {
        // Given
        let record_batches = write_test_data_to_cluster_metadata_log_file()?;
        let (topic_id, topic_name) = select_topic_from_metadata(record_batches);
        let record_value = b"Hello Reverse Engineering!";
        let partition_data_bytes: [&[u8]; 3] = [
            b"\0\0\0\0\0\0\0\0\0\0\0R\0\0\0\0\x02\x8b\xaa\x87*\0\0\0\0\0\0\0\0\x01\x91\xe0[m\x8b\0\
            \0\x01\x91\xe0[m\x8b\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01@\0\0\0\x014",
            record_value,
            b"\0",
        ];
        let partition_data = partition_data_bytes.concat();
        write_test_data_to_partition_log_file(&topic_name, 0, &Bytes::from(partition_data))?;
        let topics = &[topic_id];

        // When
        let mut fetch_response = perform_fetch_request(topics.to_vec())?;

        // Then
        assert_eq!(fetch_response.throttle_time_ms, 0);
        assert_eq!(fetch_response.session_id, 0);
        assert_eq!(fetch_response.responses.elements.len(), 1);
        let mut response = fetch_response.responses.elements.remove(0);
        assert_eq!(response.topic_id, topic_id);
        assert_eq!(response.partitions.elements.len(), 1);
        let mut partition_data = response.partitions.elements.remove(0);
        assert_eq!(&partition_data.error_code, &ErrorCode::None);
        let value = partition_data
            .records
            .record_batches
            .elements
            .remove(0)
            .records
            .elements
            .remove(0)
            .value
            .into_inner()
            .raw_record_value()
            .unwrap();
        assert_eq!(&value, &record_value[..]);
        Ok(())
    }

    #[test]
    fn test_fetch_v16_topic_with_multiple_records() -> Result<()> {
        // Given
        let record_batches = write_test_data_to_cluster_metadata_log_file()?;
        println!("decoded metadata record_batches");
        let (topic_id, topic_name) = select_topic_from_metadata(record_batches);
        let first_record_value = b"Hello Earth!";
        let second_record_value = b"Hello World!";
        let partition_data_bytes: [&[u8]; 5] = [
            b"\0\0\0\0\0\0\0\0\0\0\0D\0\0\0\0\x02da|J\0\0\0\0\0\0\0\0\x01\x91\xe0[m\x8b\0\0\x01\x91\
              \xe0[m\x8b\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01$\0\0\0\x01\x18",
            first_record_value,
            b"\0\0\0\0\0\0\0\0\x01\0\0\0D\0\0\0\0\x02\x98\xec\x18\xd3\0\0\0\0\0\0\0\0\x01\x91\xe0[m\
              \x8b\0\0\x01\x91\xe0[m\x8b\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01$\0\0\0\x01\x18",
            second_record_value,
            b"\0",
        ];
        let partition_data = partition_data_bytes.concat();
        println!("Now decoding partition record_batches");
        write_test_data_to_partition_log_file(&topic_name, 0, &Bytes::from(partition_data))?;
        let topics = &[topic_id];

        // When
        let mut fetch_response = perform_fetch_request(topics.to_vec())?;

        // Then
        assert_eq!(fetch_response.throttle_time_ms, 0);
        assert_eq!(fetch_response.session_id, 0);
        assert_eq!(fetch_response.responses.elements.len(), 1);
        let mut response = fetch_response.responses.elements.remove(0);
        assert_eq!(response.topic_id, topic_id);
        assert_eq!(response.partitions.elements.len(), 1);
        let partition_data = response.partitions.elements.remove(0);
        assert_eq!(&partition_data.error_code, &ErrorCode::None);
        let mut record_batches = partition_data.records.record_batches.elements;
        let first_value = record_batches
            .remove(0)
            .records
            .elements
            .remove(0)
            .value
            .into_inner()
            .raw_record_value()
            .unwrap();
        assert_eq!(&first_value, &first_record_value[..]);
        let second_value = record_batches
            .remove(0)
            .records
            .elements
            .remove(0)
            .value
            .into_inner()
            .raw_record_value()
            .unwrap();
        assert_eq!(&second_value, &second_record_value[..]);
        Ok(())
    }

    fn select_topic_from_metadata(record_batches: Contiguous<RecordBatch>) -> (uuid::Uuid, Bytes) {
        record_batches
            .metadata_record_messages()
            .into_iter()
            .find_map(|record_message| {
                if let MetadataRecordMessage::TopicRecord(topic_record) = record_message {
                    Some((topic_record.topic_id, topic_record.name.value))
                } else {
                    None
                }
            })
            .unwrap()
    }

    fn perform_fetch_request(topics: Vec<Uuid>) -> Result<FetchResponse> {
        let fetch_request = FetchRequest::new(topics);
        let response = perform_request(ApiKind::Fetch, 16, &fetch_request)?;
        let fetch_response = response.message.payload().get_decoded();
        Ok(fetch_response)
    }
}
