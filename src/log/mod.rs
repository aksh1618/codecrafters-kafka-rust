use std::{fs, path::Path};

use crate::{
    buf::{self, BufExt as _, BufMutExt as _},
    model::*,
};
use anyhow::{ensure, Context as _};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use encode_decode_derive::{Decode, Encode};
use smart_default::SmartDefault;
use strum::{Display, EnumIter, FromRepr};

pub const CLUSTER_METADATA_PATH: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
pub const PARTITION_LOG_BASE_DIR_PATH: &str = "/tmp/kraft-combined-logs";

pub fn partition_log_path(topic_name: &Bytes, partition_id: i32) -> String {
    // Ideally the offset should be an argument, but let's hardcode for now
    let offset = "00000000000000000000";
    let topic_name = &String::from_utf8_lossy(topic_name);
    format!("{PARTITION_LOG_BASE_DIR_PATH}/{topic_name}-{partition_id}/{offset}.log")
}

pub fn read_records(path: &Path) -> anyhow::Result<RecordBatches> {
    ensure!(path.is_file(), "record path must be a file");
    let buf = fs::read(path)?;
    let record_batches: RecordBatches = buf.as_slice().get_decoded();
    Ok(record_batches)
}

pub fn read_metadata_records() -> anyhow::Result<Vec<MetadataRecordMessage>> {
    read_records(Path::new(CLUSTER_METADATA_PATH)).map(RecordBatches::metadata_record_messages)
}

pub fn read_raw_records(topic_name: &Bytes, partition_id: i32) -> anyhow::Result<RecordBatches> {
    let partition_log_path = partition_log_path(topic_name, partition_id);
    read_records(Path::new(&partition_log_path))
        .with_context(|| format!("Failed to read records from log path {partition_log_path}"))
}

macro_rules! define_metadata_record_types {
    {$($variant:ident = $value:expr),*$(,)?} => {
        #[derive(Display, Debug, Clone, Copy, FromRepr, EnumIter, Default)]
        #[repr(u8)]
        #[expect(clippy::enum_variant_names, reason = "want to keep name same as those in kafka code")]
        pub enum MetadataRecordKind {
            #[default]
            $(
                $variant = $value,
            )*
        }

        #[derive(Debug, SmartDefault)]
        #[expect(clippy::enum_variant_names, reason = "want to keep name same as those in kafka code")]
        pub enum MetadataRecordMessage {
            #[default]
            $(
                $variant($variant),
            )*
        }

        impl MetadataRecordMessage {
            pub const fn record_kind(&self) -> MetadataRecordKind {
                match self {
                    $(
                        MetadataRecordMessage::$variant(_) => MetadataRecordKind::$variant,
                    )*
                }
            }
        }

        impl buf::Encode for MetadataRecordValue {
            fn encode<B: BufMut + ?Sized>(&self, mut buf: &mut B) {
                buf.put_encoded(&self.data_frame_version);
                buf.put_encoded(&self.api_key);
                buf.put_encoded(&self.version);
                match &self.message {
                    $(
                        MetadataRecordMessage::$variant(record) => buf.put_encoded(record),
                    )*
                }
            }
        }

        fn decode_metadata_record_message<B: Buf + ?Sized>(
            mut buf: &mut B,
            record_kind: MetadataRecordKind,
        ) -> MetadataRecordMessage {
            match record_kind {
                $(
                    MetadataRecordKind::$variant => MetadataRecordMessage::$variant(buf.get_decoded()),
                )*
            }
        }
    }
}

// NOTE: This is sensitive to ordering, as the first one is being assigned #[default]
define_metadata_record_types! {
    FeatureLevelRecord = 12,
    TopicRecord = 2,
    PartitionRecord = 3,
}

impl From<MetadataRecordKind> for MetadataRecordApiKey {
    fn from(record_kind: MetadataRecordKind) -> Self {
        record_kind as Self
    }
}

impl TryFrom<MetadataRecordApiKey> for MetadataRecordKind {
    type Error = ErrorCode;

    fn try_from(api_key: MetadataRecordApiKey) -> Result<Self> {
        Self::from_repr(api_key).ok_or_else(|| {
            eprintln!("Unknown record api key: {api_key}");
            ErrorCode::InvalidRequest
        })
    }
}

pub type RecordBatches = Contiguous<RecordBatch>;

impl RecordBatches {
    #[must_use]
    pub fn metadata_record_messages(self) -> Vec<MetadataRecordMessage> {
        self.elements
            .into_iter()
            .flat_map(|record_batch| record_batch.records.elements)
            .map(|record| record.value.into_inner())
            .filter_map(RecordValue::metadata_record_value)
            .map(|record_value| record_value.message)
            .collect::<Vec<_>>()
    }
}

#[derive(Debug, Default, Decode)]
pub struct RecordBatch {
    pub base_offset: Int64,
    pub length: Int32,
    pub partition_leader_epoch: Int32,
    pub magic: Int8,
    pub crc: UInt32,
    pub attributes: Int16,
    pub last_offset_delta: Int32,
    pub base_timestamp: Int64,
    pub max_timestamp: Int64,
    pub producer_id: Int64,
    pub producer_epoch: Int16,
    pub base_sequence: Int32,
    // pub records_count: Int32,
    pub records: Array<Record>,
}

// Custom implementation, as we need to update the crc
impl buf::Encode for RecordBatch {
    fn encode<B: BufMut + ?Sized>(&self, mut buf: &mut B) {
        // First use temp buffer for all content required for calculating crc
        let mut temp_buf = BytesMut::new();
        temp_buf.put_encoded(&self.attributes);
        temp_buf.put_encoded(&self.last_offset_delta);
        temp_buf.put_encoded(&self.base_timestamp);
        temp_buf.put_encoded(&self.max_timestamp);
        temp_buf.put_encoded(&self.producer_id);
        temp_buf.put_encoded(&self.producer_epoch);
        temp_buf.put_encoded(&self.base_sequence);
        temp_buf.put_encoded(&self.records);
        // Then calculate the crc for temp buffer
        let updated_crc = crc32c::crc32c(&temp_buf);
        // Then encode everything before the crc
        buf.put_encoded(&self.base_offset);
        buf.put_encoded(&self.length);
        buf.put_encoded(&self.partition_leader_epoch);
        buf.put_encoded(&self.magic);
        // Then encode the updated crc
        buf.put_encoded(&updated_crc);
        // Then encode the remaining content from temp buffer
        buf.put_encoded(&temp_buf.freeze());
    }
}

#[derive(Debug, Default, Encode, Decode)]
pub struct Record {
    pub length: Varint,
    pub attributes: Int8,
    pub timestamp_delta: Varlong,
    pub offset_delta: Varint,
    // pub key_length: Varint,
    pub key: VarintBytes,
    // pub value_length: Varint,
    pub value: VarintSized<RecordValue>,
    // pub headers_count: Varint,
    pub headers: VarintArray<RecordHeader>,
}

#[derive(Debug, SmartDefault)]
pub enum RecordValue {
    #[default]
    MetadataRecordValue(MetadataRecordValue),
    RawRecordValue(Bytes),
}

// "filter-map" implementations for enum variants!
// These allow for usage of filter_map similar to filter_map(Result::ok)
impl RecordValue {
    pub fn raw_record_value(self) -> Option<Bytes> {
        match self {
            Self::MetadataRecordValue(_) => None,
            Self::RawRecordValue(value) => Some(value),
        }
    }

    pub fn metadata_record_value(self) -> Option<MetadataRecordValue> {
        match self {
            Self::MetadataRecordValue(value) => Some(value),
            Self::RawRecordValue(_) => None,
        }
    }
}

impl buf::Encode for RecordValue {
    fn encode<B: BufMut + ?Sized>(&self, mut buf: &mut B) {
        match self {
            Self::MetadataRecordValue(value) => buf.put_encoded(value),
            Self::RawRecordValue(value) => buf.put_encoded(value),
        }
    }
}

impl buf::Decode for VarintSized<RecordValue> {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        let value_length: Varint = buf.get_decoded();
        let probably_data_frame_version: UnsignedVarint = buf.get_decoded();
        let probably_api_key: MetadataRecordApiKey = buf.get_decoded();
        let record_kind = MetadataRecordKind::try_from(probably_api_key);
        if let Ok(record_kind) = record_kind {
            // We have a metadata record!
            let data_frame_version = probably_data_frame_version;
            let api_key = probably_api_key;
            let version = buf.get_decoded();
            let message = decode_metadata_record_message(buf, record_kind);
            RecordValue::MetadataRecordValue(MetadataRecordValue {
                data_frame_version,
                api_key,
                version,
                message,
            })
            .into()
        } else {
            // We don't have a metadata record! Let's parse a raw record
            let value_length =
                usize::try_from(value_length.into_inner()).expect("length should be non-negative");
            let mut bytes = BytesMut::with_capacity(value_length);
            // First need to put back the bytes read for data frame version and api key
            bytes.put_encoded(&probably_data_frame_version);
            bytes.put_encoded(&probably_api_key);
            // And then the rest of the bytes
            let remaining_bytes_length: usize = value_length
                - size_of_val(&probably_data_frame_version)
                - size_of_val(&probably_api_key);
            let remaining_bytes = buf.copy_to_bytes(remaining_bytes_length);
            bytes.put_encoded(&remaining_bytes);
            let bytes = bytes.freeze();
            RecordValue::RawRecordValue(bytes).into()
        }
    }
}

#[derive(Debug, Default)]
pub struct MetadataRecordValue {
    pub data_frame_version: UnsignedVarint,
    pub api_key: MetadataRecordApiKey,
    pub version: UnsignedVarint,
    pub message: MetadataRecordMessage,
}

pub type MetadataRecordApiKey = UnsignedVarint;

#[derive(Debug, Default, Encode, Decode)]
pub struct RecordHeader {
    // pub key_length: Varint,
    pub key: CompactString,
    // pub value_length: Varint,
    pub value: CompactBytes,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct TopicRecord {
    pub name: CompactString,
    pub topic_id: Uuid,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct PartitionRecord {
    pub partition_id: Int32,
    pub topic_id: Uuid,
    pub replicas: CompactArray<Int32>,
    pub isr: CompactArray<Int32>,
    pub removing_replicas: CompactArray<Int32>,
    pub adding_replicas: CompactArray<Int32>,
    pub leader: Int32,
    pub leader_epoch: Int32,
    pub partition_epoch: Int32,
    pub directories: CompactArray<Uuid>,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Default, Encode, Decode)]
pub struct FeatureLevelRecord {
    pub name: CompactString,
    pub feature_level: Int16,
    pub tag_buffer: TagBuffer,
}

#[allow(clippy::restriction)]
#[cfg(test)]
pub mod test {
    use std::{fs, io::Write as _};

    use super::*;

    #[test]
    fn test_record_batch_read() -> anyhow::Result<()> {
        let record_batches = write_test_data_to_cluster_metadata_log_file()?;
        // dbg!(&record_batches);
        assert!(!record_batches.elements.is_empty());
        Ok(())
    }

    pub fn write_test_data_to_cluster_metadata_log_file() -> Result<RecordBatches> {
        let data: &[u8] = b"\
            \x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x4f\x00\x00\x00\x01\x02\xb0\x69\x45\x7c\
            \x00\x00\x00\x00\x00\x00\x00\x00\x01\x91\xe0\x5a\xf8\x18\x00\x00\x01\x91\xe0\x5a\xf8\
            \x18\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01\x3a\x00\
            \x00\x00\x01\x2e\x01\x0c\x00\x11\x6d\x65\x74\x61\x64\x61\x74\x61\x2e\x76\x65\x72\x73\
            \x69\x6f\x6e\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x9a\x00\x00\
            \x00\x01\x02\x1f\x3f\xdc\xa6\x00\x00\x00\x00\x00\x01\x00\x00\x01\x91\xe0\x5b\x2d\x15\
            \x00\x00\x01\x91\xe0\x5b\x2d\x15\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\
            \xff\x00\x00\x00\x02\x3c\x00\x00\x00\x01\x30\x01\x02\x00\x04\x62\x61\x72\x00\x00\x00\
            \x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x28\x00\x00\x90\x01\x00\x00\x02\x01\
            \x82\x01\x01\x03\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\
            \x00\x00\x00\x28\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x00\x00\x00\x01\x00\
            \x00\x00\x00\x00\x00\x00\x00\x02\x10\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\
            \x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x9a\x00\x00\x00\x01\
            \x02\x22\xf5\x1c\x5b\x00\x00\x00\x00\x00\x01\x00\x00\x01\x91\xe0\x5b\x2d\x15\x00\x00\
            \x01\x91\xe0\x5b\x2d\x15\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\
            \x00\x00\x02\x3c\x00\x00\x00\x01\x30\x01\x02\x00\x04\x66\x6f\x6f\x00\x00\x00\x00\x00\
            \x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x21\x00\x00\x90\x01\x00\x00\x02\x01\x82\x01\
            \x01\x03\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\
            \x00\x21\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x00\x00\x00\x01\x00\x00\x00\
            \x00\x00\x00\x00\x00\x02\x10\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\
            \x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\xe4\x00\x00\x00\x01\x02\xdf\
            \x30\x50\xe2\x00\x00\x00\x00\x00\x02\x00\x00\x01\x91\xe0\x5b\x2d\x15\x00\x00\x01\x91\
            \xe0\x5b\x2d\x15\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\
            \x03\x3c\x00\x00\x00\x01\x30\x01\x02\x00\x04\x70\x61\x7a\x00\x00\x00\x00\x00\x00\x40\
            \x00\x80\x00\x00\x00\x00\x00\x00\x53\x00\x00\x90\x01\x00\x00\x02\x01\x82\x01\x01\x03\
            \x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x53\
            \x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\
            \x00\x00\x00\x02\x10\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x01\x00\
            \x00\x90\x01\x00\x00\x04\x01\x82\x01\x01\x03\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\
            \x00\x40\x00\x80\x00\x00\x00\x00\x00\x00\x53\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\
            \x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02\x10\x00\x00\x00\x00\x00\
            \x40\x00\x80\x00\x00\x00\x00\x00\x00\x01\x00\x00";
        if let Some(parent) = Path::new(CLUSTER_METADATA_PATH).parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::File::create(CLUSTER_METADATA_PATH)?;
        file.write_all(data)?;
        let record_batches = read_records(Path::new(CLUSTER_METADATA_PATH)).unwrap();
        Ok(record_batches)
    }

    pub fn write_test_data_to_partition_log_file(
        topic_name: &Bytes,
        partition_id: i32,
        data: &Bytes,
    ) -> Result<RecordBatches> {
        let partition_log_path = partition_log_path(topic_name, partition_id);
        if let Some(parent) = Path::new(&partition_log_path).parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::File::create(&partition_log_path)?;
        file.write_all(data)?;
        let record_batches = read_records(Path::new(&partition_log_path)).unwrap();
        Ok(record_batches)
    }
}
