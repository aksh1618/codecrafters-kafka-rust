#![allow(dead_code, reason = "Prototyping")] // TODO: Remove this when done prototyping

use std::{fs, path::Path};

use crate::{
    buf::{self, BufExt as _, BufMutExt as _},
    model::*,
};
use anyhow::ensure;
use bytes::{Buf, BufMut};
use encode_decode_derive::{Decode, Encode};
use strum::{Display, EnumIter, FromRepr};

pub const CLUSTER_METADATA_PATH: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

pub fn read_records(path: &Path) -> anyhow::Result<RecordBatches> {
    ensure!(path.is_file(), "record path must be a file");
    let buf = fs::read(path)?;
    let record_batches: RecordBatches = buf.as_slice().get_decoded();
    Ok(record_batches)
}

macro_rules! define_record_types {
    {$($variant:ident = $value:expr),*$(,)?} => {
        #[derive(Display, Debug, Clone, Copy, FromRepr, EnumIter)]
        #[repr(u8)]
        #[expect(clippy::enum_variant_names, reason = "want to keep name same as those in kafka code")]
        pub enum RecordKind {
            $(
                $variant = $value,
            )*
        }

        #[derive(Debug)]
        #[expect(clippy::enum_variant_names, reason = "want to keep name same as those in kafka code")]
        pub enum RecordMessage {
            $(
                $variant($variant),
            )*
        }

        impl RecordMessage {
            pub const fn record_kind(&self) -> RecordKind {
                match self {
                    $(
                        RecordMessage::$variant(_) => RecordKind::$variant,
                    )*
                }
            }
        }

        impl buf::Encode for RecordValue {
            fn encode<B: BufMut + ?Sized>(&self, mut buf: &mut B) {
                buf.put_encoded(&self.data_frame_version);
                buf.put_encoded(&self.api_key);
                buf.put_encoded(&self.version);
                match &self.message {
                    $(
                        RecordMessage::$variant(record) => buf.put_encoded(record),
                    )*
                }
            }
        }

        impl buf::Decode for RecordValue {
            fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
                let data_frame_version = buf.get_decoded();
                let api_key = buf.get_decoded();
                let version = buf.get_decoded();
                let record_kind = RecordKind::try_from(api_key).unwrap();
                let message = match record_kind {
                    $(
                        RecordKind::$variant => RecordMessage::$variant(buf.get_decoded()),
                    )*
                };
                Self {
                    data_frame_version,
                    api_key,
                    version,
                    message,
                }
            }
        }
    }
}

define_record_types! {
    TopicRecord = 2,
    PartitionRecord = 3,
    FeatureLevelRecord = 12,
}

impl Default for RecordKind {
    fn default() -> Self {
        Self::FeatureLevelRecord
    }
}

impl Default for RecordMessage {
    fn default() -> Self {
        Self::FeatureLevelRecord(FeatureLevelRecord::default())
    }
}

impl From<RecordKind> for RecordApiKey {
    fn from(record_kind: RecordKind) -> Self {
        record_kind as Self
    }
}

impl TryFrom<RecordApiKey> for RecordKind {
    type Error = ErrorCode;

    fn try_from(api_key: RecordApiKey) -> Result<Self> {
        Self::from_repr(api_key).ok_or_else(|| {
            eprintln!("Unknown record api key: {api_key}");
            ErrorCode::InvalidRequest
        })
    }
}

pub type RecordBatches = Contiguous<RecordBatch>;

#[derive(Debug, Default, Encode, Decode)]
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

#[derive(Debug, Default, Encode, Decode)]
pub struct Record {
    pub length: Varint,
    pub attributes: Int8,
    pub timestamp_delta: Varlong,
    pub offset_delta: Varint,
    // pub key_length: Varint,
    pub key: VarintBytes,
    pub value_length: Varint,
    pub value: RecordValue,
    // pub headers_count: Varint,
    pub headers: VarintArray<RecordHeader>,
}

#[derive(Debug, Default)]
pub struct RecordValue {
    pub data_frame_version: UnsignedVarint,
    pub api_key: RecordApiKey,
    pub version: UnsignedVarint,
    // pub message: RecordMessage,
    pub message: RecordMessage,
}

pub type RecordApiKey = UnsignedVarint;
// pub type RecordMessage = Bytes;

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
        write_test_data_to_cluster_metadata_log_file()?;

        let record_batches = read_records(Path::new(CLUSTER_METADATA_PATH))?;
        // dbg!(&record_batches);
        assert!(!record_batches.elements.is_empty());
        Ok(())
    }

    pub fn write_test_data_to_cluster_metadata_log_file() -> Result<()> {
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
        Ok(())
    }
}
