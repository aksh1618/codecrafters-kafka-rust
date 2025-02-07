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
    dbg!(&request);
    if let Some(error_code) = validate(request) {
        return Err(error_code);
    }
    let mut buf = BytesMut::new();
    let describe_partitions_request: DescribeTopicPartitionsRequest = request.payload.get_decoded();
    dbg!(&describe_partitions_request);
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
    dbg!(&topics_response);
    let response = DescribeTopicPartitionsResponse {
        topics: topics_response.into(),
        ..Default::default()
    };
    buf.put_encoded(&response);
    Ok(buf.into())
}

#[derive(Debug, Default, Decode)]
struct DescribeTopicPartitionsRequest {
    topics: CompactArray<TopicRequest>,
    response_partition_limit: Int32,
    cursor: Option<Cursor>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Decode)]
struct TopicRequest {
    name: CompactString,
    tag_buffer: TagBuffer,
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json
#[derive(Debug, Default, Encode)]
struct DescribeTopicPartitionsResponse {
    throttle_time_ms: Int32,
    topics: CompactArray<DescribeTopicPartitionsResponseTopic>,
    next_cursor: Option<Cursor>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode)]
struct DescribeTopicPartitionsResponseTopic {
    error_code: ErrorCode,
    name: CompactNullableString,
    topic_id: Uuid,
    is_internal: Boolean,
    partitions: CompactArray<DescribeTopicPartitionsResponsePartition>,
    topic_authorized_operations: Int32,
    tag_buffer: TagBuffer,
}

// See also:
// - https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas
// - https://jack-vanlightly.com/blog/2023/8/17/kafka-kip-966-fixing-the-last-replica-standing-issue
#[derive(Debug, Default, Encode)]
struct DescribeTopicPartitionsResponsePartition {
    error_code: ErrorCode,
    partition_index: Int32,
    leader_id: Int32,
    leader_epoch: Int32,
    replica_nodes: CompactArray<Int32>,
    isr_nodes: CompactArray<Int32>, // isr -> in sync replicas
    eligible_leader_replicas: CompactArray<Int32>,
    last_known_elr: CompactArray<Int32>, // elr -> eligile leader replicas
    offline_replicas: CompactArray<Int32>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
struct Cursor {
    topic_name: CompactString,
    partition_index: Int32,
    tag_buffer: TagBuffer,
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
