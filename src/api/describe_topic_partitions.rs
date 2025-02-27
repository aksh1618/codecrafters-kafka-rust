use super::{RequestMessageV2, ResponsePayload};
use crate::api::{ApiKind, KafkaBrokerApi};
use crate::buf::{self, BufExt as _, BufMutExt as _};
use crate::model::*;
use bytes::{Buf, BufMut, BytesMut};
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
    let topics_response = describe_partitions_request
        .topics
        .elements
        .into_iter()
        .map(|topic| DescribeTopicPartitionsResponseTopic {
            error_code: ErrorCode::UnknownTopicOrPartition,
            name: CompactNullableString {
                length: topic.name.length,
                value: topic.name.value,
            },
            ..Default::default()
        })
        .collect::<Vec<_>>();
    // dbg!(&topics_response);
    let response = DescribeTopicPartitionsResponse {
        topics: topics_response.into(),
        ..Default::default()
    };
    buf.put_encoded(&response);
    Ok(buf.into())
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

#[allow(clippy::restriction)]
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
    use std::io::Result;
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
