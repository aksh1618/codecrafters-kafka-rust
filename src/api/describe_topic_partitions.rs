use std::collections::HashMap;
use std::path::Path;

use super::{RequestMessageV2, ResponsePayload};
use crate::api::{ApiKind, KafkaBrokerApi};
use crate::buf::{self, BufExt as _, BufMutExt as _};
use crate::log::{
    read_records, PartitionRecord, RecordMessage, TopicRecord, CLUSTER_METADATA_PATH,
};
use crate::model::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use encode_decode_derive::{Decode, Encode};

// #[derive(Debug, Clone, Copy, Default)]
pub struct DescribeTopicPartitionsV0;

impl KafkaBrokerApi for DescribeTopicPartitionsV0 {
    fn kind(&self) -> ApiKind {
        ApiKind::DescribeTopicPartitions
    }

    fn api_version(&self) -> ApiVersion {
        0
    }

    fn handle_request(&self, mut request: RequestMessageV2) -> Result<ResponsePayload> {
        create_response(&mut request)
    }
}

fn validate(request_message: &RequestMessageV2) -> Option<ErrorCode> {
    if DescribeTopicPartitionsV0.api_version() != request_message.header.api_version {
        return Some(ErrorCode::UnsupportedVersion);
    }
    None
}

fn create_response(request: &mut RequestMessageV2) -> Result<ResponsePayload> {
    // dbg!(&request);
    if let Some(error_code) = validate(request) {
        return Err(error_code);
    }
    let mut buf = BytesMut::new();
    let describe_partitions_request: DescribeTopicPartitionsRequest = request.payload.get_decoded();
    // dbg!(&describe_partitions_request);
    let record_batches = read_records(Path::new(CLUSTER_METADATA_PATH)).map_err(|e| {
        println!("Failed reading cluster metadata file from {CLUSTER_METADATA_PATH}: {e}");
        ErrorCode::UnknownServerError
    })?;
    let mut topic_records = HashMap::new();
    let mut topic_wise_partition_records = HashMap::new();
    #[expect(
        clippy::match_wildcard_for_single_variants,
        reason = "we only care about topic and partition records"
    )]
    record_batches
        .elements
        .into_iter()
        .flat_map(|record_batch| record_batch.records.elements.into_iter())
        .for_each(|record| match record.value.message {
            RecordMessage::TopicRecord(topic_record) => {
                let topic_name = topic_record
                    .name
                    .value
                    .as_ref()
                    .expect("topic name should be present in a topic record")
                    .clone();
                topic_records.insert(topic_name, topic_record);
            }
            RecordMessage::PartitionRecord(partition_record) => {
                topic_wise_partition_records
                    .entry(partition_record.topic_id)
                    .or_insert_with(Vec::new)
                    .push(partition_record);
            }
            _ => (),
        });

    let topics_response = describe_partitions_request
        .topics
        .elements
        .into_iter()
        .map(|topic| {
            let topic_name = topic
                .name
                .value
                .expect("topic name should be present in a topic record");
            get_topic_response_for_topic(
                topic_name,
                &mut topic_records,
                &mut topic_wise_partition_records,
            )
        })
        .collect::<Vec<_>>();
    let response = DescribeTopicPartitionsResponse {
        topics: topics_response.into(),
        ..Default::default()
    };
    buf.put_encoded(&response);
    Ok(buf.into())
}

fn get_topic_response_for_topic(
    topic_name: Bytes,
    topic_records: &mut HashMap<Bytes, TopicRecord>,
    topic_wise_partition_records: &mut HashMap<Uuid, Vec<PartitionRecord>>,
) -> DescribeTopicPartitionsResponseTopic {
    topic_records
        .remove(&topic_name)
        .map(|topic_record| {
            get_topic_response_for_topic_and_partitions(topic_record, topic_wise_partition_records)
        })
        .unwrap_or_else(|| DescribeTopicPartitionsResponseTopic {
            error_code: ErrorCode::UnknownTopicOrPartition,
            name: topic_name.into(),
            ..Default::default()
        })
}

fn get_topic_response_for_topic_and_partitions(
    topic_record: TopicRecord,
    topic_wise_partition_records: &mut HashMap<Uuid, Vec<PartitionRecord>>,
) -> DescribeTopicPartitionsResponseTopic {
    DescribeTopicPartitionsResponseTopic {
        error_code: ErrorCode::None,
        name: topic_record
            .name
            .value
            .expect("topic name should be present in a topic record")
            .into(),
        topic_id: topic_record.topic_id,
        partitions: get_partition_response_for_topic(
            topic_record.topic_id,
            topic_wise_partition_records,
        )
        .into(),
        ..Default::default()
    }
}

fn get_partition_response_for_topic(
    topic_id: Uuid,
    topic_wise_partition_records: &mut HashMap<Uuid, Vec<PartitionRecord>>,
) -> Vec<DescribeTopicPartitionsResponsePartition> {
    topic_wise_partition_records
        .remove(&topic_id)
        .map(|partition_records| {
            partition_records
                .into_iter()
                .map(get_partition_response_for_partition)
                .collect::<Vec<_>>()
        })
        // TODO: Currently returning empty vec, see if ErrorCode::UnknownTopicOrPartition is required
        .unwrap_or_default()
}

fn get_partition_response_for_partition(
    partition_record: PartitionRecord,
) -> DescribeTopicPartitionsResponsePartition {
    DescribeTopicPartitionsResponsePartition {
        error_code: ErrorCode::None,
        partition_index: partition_record.partition_id,
        leader_id: partition_record.leader,
        leader_epoch: partition_record.leader_epoch,
        replica_nodes: partition_record.replicas,
        isr_nodes: partition_record.isr,
        ..Default::default()
    }
}

#[derive(Debug, Default, Encode, Decode)]
pub struct DescribeTopicPartitionsRequest {
    topics: CompactArray<TopicRequest>,
    response_partition_limit: Int32,
    cursor: Option<Cursor>,
    tag_buffer: TagBuffer,
}

impl DescribeTopicPartitionsRequest {
    pub fn new(topics: Vec<String>) -> Self {
        Self {
            topics: CompactArray::from(
                topics
                    .into_iter()
                    .map(|topic| TopicRequest {
                        name: CompactString::from(topic.as_str()),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>(),
            ),
            ..Default::default()
        }
    }
}

#[derive(Debug, Default, Encode, Decode)]
struct TopicRequest {
    name: CompactString,
    tag_buffer: TagBuffer,
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json
#[derive(Debug, Default, Encode, Decode)]
pub struct DescribeTopicPartitionsResponse {
    pub throttle_time_ms: Int32,
    pub topics: CompactArray<DescribeTopicPartitionsResponseTopic>,
    pub next_cursor: Option<Cursor>,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct DescribeTopicPartitionsResponseTopic {
    pub error_code: ErrorCode,
    pub name: CompactNullableString,
    pub topic_id: Uuid,
    pub is_internal: Boolean,
    pub partitions: CompactArray<DescribeTopicPartitionsResponsePartition>,
    pub topic_authorized_operations: Int32,
    pub tag_buffer: TagBuffer,
}

// See also:
// - https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas
// - https://jack-vanlightly.com/blog/2023/8/17/kafka-kip-966-fixing-the-last-replica-standing-issue
#[derive(Debug, Default, Encode, Decode)]
pub struct DescribeTopicPartitionsResponsePartition {
    pub error_code: ErrorCode,
    pub partition_index: Int32,
    pub leader_id: Int32,
    pub leader_epoch: Int32,
    pub replica_nodes: CompactArray<Int32>,
    pub isr_nodes: CompactArray<Int32>, // isr -> in sync replicas
    pub eligible_leader_replicas: CompactArray<Int32>,
    pub last_known_elr: CompactArray<Int32>, // elr -> eligile leader replicas
    pub offline_replicas: CompactArray<Int32>,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct Cursor {
    pub topic_name: CompactString,
    pub partition_index: Int32,
    pub tag_buffer: TagBuffer,
}

impl buf::Encode for Option<Cursor> {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        match self {
            Some(cursor) => buf.put_encoded(cursor),
            None => buf.put_u8(0xff),
        }
    }
}

impl buf::Decode for Option<Cursor> {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        if buf.get_u8() == 0xff {
            return None;
        }
        Some(Cursor {
            topic_name: buf.get_decoded(),
            partition_index: buf.get_i32(),
            tag_buffer: buf.get_i8(),
        })
    }
}

#[allow(
    clippy::restriction,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        api::{
            describe_topic_partitions::{
                DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
            },
            ApiKind,
        },
        log::test::write_test_data_to_cluster_metadata_log_file,
        server::tests::perform_request,
    };
    use anyhow::Result;
    use uuid::Uuid;

    #[test]
    fn test_describe_topic_partitions_v0_unknown_topic() -> Result<()> {
        // Given
        const TOPIC_NAME: &str = "unknown-topic-paz";

        // When
        let describe_topic_partitions_response =
            perform_describe_topic_partitions_request(vec![TOPIC_NAME.to_string()])?;

        // Then
        assert_eq!(describe_topic_partitions_response.topics.length, 1);
        let topic_details = &describe_topic_partitions_response.topics.elements[0];
        assert_eq!(topic_details.error_code, ErrorCode::UnknownTopicOrPartition);
        assert_eq!(topic_details.name.value.as_ref().unwrap(), TOPIC_NAME);
        assert_eq!(topic_details.topic_id, Uuid::nil());
        assert_eq!(topic_details.partitions.length, 0);
        assert!(topic_details.partitions.elements.is_empty());
        Ok(())
    }

    #[test]
    fn test_describe_topic_partitions_single_known_topic() -> Result<()> {
        // Given
        const TOPIC_NAME: &str = "paz";
        write_test_data_to_cluster_metadata_log_file().unwrap();

        // When
        let describe_topic_partitions_response =
            perform_describe_topic_partitions_request(vec![TOPIC_NAME.to_string()])?;

        // Then
        assert_eq!(describe_topic_partitions_response.topics.length, 1);
        let topic_details = &describe_topic_partitions_response.topics.elements[0];
        // dbg!(&topic_details);
        assert_eq!(topic_details.error_code, ErrorCode::None);
        assert_eq!(topic_details.name.value.as_ref().unwrap(), TOPIC_NAME);
        assert_eq!(
            topic_details.topic_id,
            Uuid::try_parse("00000000-0000-4000-8000-000000000053")?
        );
        assert_eq!(topic_details.partitions.length, 2);
        topic_details
            .partitions
            .elements
            .iter()
            .enumerate()
            .for_each(|(i, partition_details)| {
                assert_eq!(partition_details.error_code, ErrorCode::None);
                assert_eq!(partition_details.partition_index, i as i32);
            });
        Ok(())
    }

    #[test]
    fn test_describe_topic_partitions_multiple_known_topics() -> Result<()> {
        // Given
        let topic_names = ["foo".to_string(), "bar".to_string()];
        write_test_data_to_cluster_metadata_log_file().unwrap();
        let expected_uuids = [
            Uuid::try_parse("00000000-0000-4000-8000-000000000021")?,
            Uuid::try_parse("00000000-0000-4000-8000-000000000028")?,
        ];

        // When
        let describe_topic_partitions_response =
            perform_describe_topic_partitions_request(topic_names.to_vec())?;

        // Then
        assert_eq!(describe_topic_partitions_response.topics.length, 2);
        let topic_details = &describe_topic_partitions_response.topics.elements;
        // dbg!(&topic_details);
        topic_details
            .iter()
            .enumerate()
            .for_each(|(i, topic_details)| {
                assert_eq!(topic_details.error_code, ErrorCode::None);
                assert_eq!(
                    topic_details.name.value.as_ref().unwrap(),
                    topic_names[i].as_str()
                );
                assert_eq!(topic_details.topic_id, expected_uuids[i]);
                assert_eq!(topic_details.partitions.length, 1);
                topic_details
                    .partitions
                    .elements
                    .iter()
                    .enumerate()
                    .for_each(|(i, partition_details)| {
                        assert_eq!(partition_details.error_code, ErrorCode::None);
                        assert_eq!(partition_details.partition_index, i as i32);
                    });
            });
        Ok(())
    }

    fn perform_describe_topic_partitions_request(
        topics: Vec<String>,
    ) -> Result<DescribeTopicPartitionsResponse> {
        let describe_topic_partitions_request = DescribeTopicPartitionsRequest::new(topics);
        let response = perform_request(
            ApiKind::DescribeTopicPartitions,
            0,
            &describe_topic_partitions_request,
        )?;
        let describe_topic_partitions_response = response.message.payload().get_decoded();
        Ok(describe_topic_partitions_response)
    }
}
