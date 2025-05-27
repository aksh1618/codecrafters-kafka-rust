#![allow(
    dead_code,
    reason = "Going to evolve this as we go, so allowing some dead code for now"
)]

use std::{fmt, io, ops, result};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use encode_decode_derive::{Decode, Encode};
use paste::paste;
use smart_default::SmartDefault;
use strum::{Display, FromRepr};
use thiserror::Error;

use super::buf::{BufExt as _, BufMutExt as _, Decode, Encode};

/// Error codes from the Kafka Protocol
///
/// > We use numeric codes to indicate what problem occurred on the server. These can be translated
/// > by the client into exceptions or whatever the appropriate error handling mechanism in the
/// > client language.
/// > (From [Kafka protocol doc](https://kafka.apache.org/protocol.html#protocol_error_codes))
#[repr(i16)]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Error, Display, FromRepr, Encode, Decode)]
pub enum ErrorCode {
    UnknownServerError = -1,
    #[default]
    None = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
    InvalidRequest = 42,
    UnknownTopicId = 100,
    // TODO: Add more from https://kafka.apache.org/protocol.html#protocol_error_codes
}

pub type Result<T> = result::Result<T, ErrorCode>;

impl From<ErrorCode> for Bytes {
    fn from(error_code: ErrorCode) -> Self {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_i16(error_code as i16);
        buf.freeze()
    }
}

impl From<io::Error> for ErrorCode {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::InvalidData {
            Self::InvalidRequest
        } else {
            Self::UnknownServerError
        }
    }
}

#[derive(Debug, Default)]
pub struct Contiguous<Item> {
    pub elements: Vec<Item>,
}

impl<T> From<Vec<T>> for Contiguous<T> {
    fn from(value: Vec<T>) -> Self {
        Self { elements: value }
    }
}

impl<Item: Encode> Encode for Contiguous<Item> {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        for x in &self.elements {
            buf.put_encoded(x);
        }
    }
}

impl<Item: Decode + fmt::Debug> Decode for Contiguous<Item> {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        let mut elements = Vec::new();
        while buf.has_remaining() {
            let element = buf.get_decoded();
            elements.push(element);
        }
        Self { elements }
    }
}

#[derive(Debug, Default)]
pub struct Array<Item> {
    pub elements: Vec<Item>,
}

impl<T> From<Vec<T>> for Array<T> {
    fn from(value: Vec<T>) -> Self {
        Self { elements: value }
    }
}

impl<Item: Encode> Encode for Array<Item> {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        let len = self.elements.len();
        if len == 0 {
            buf.put_i32(-1);
            return;
        }
        buf.put_i32(
            Int32::try_from(len).expect(
                "Arrays should be smaller than i32::max in length, was this supposed to be a compact array instead?",
            )
        );
        for x in &self.elements {
            buf.put_encoded(x);
        }
    }
}

impl<Item: Decode + fmt::Debug> Decode for Array<Item> {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        let length = buf.get_i32();
        if length <= 0 {
            return Self { elements: vec![] };
        }
        let mut elements =
            Vec::with_capacity(usize::try_from(length).expect("length should be non-negative"));
        for _ in 0..length {
            let element: Item = buf.get_decoded();
            elements.push(element);
        }
        Self { elements }
    }
}

#[derive(Debug, Default)]
pub struct VarintArray<Item> {
    pub elements: Vec<Item>,
}

impl<T> From<Vec<T>> for VarintArray<T> {
    fn from(value: Vec<T>) -> Self {
        Self { elements: value }
    }
}

impl<Item: Encode> Encode for VarintArray<Item> {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        let len = self.elements.len();
        if len == 0 {
            buf.put_encoded(&Varnum(-1_i32));
            return;
        }
        buf.put_encoded(
            &Varnum(i32::try_from(len).expect(
                "Varint arrays should be smaller than i8::max in length, probably time to refactor varint",
            ))
        );
        for x in &self.elements {
            buf.put_encoded(x);
        }
    }
}

impl<Item: Decode> Decode for VarintArray<Item> {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        let varint: Varint = buf.get_decoded();
        let length = varint.0;
        if length <= 0 {
            return Self { elements: vec![] };
        }
        let mut elements =
            Vec::with_capacity(usize::try_from(length).expect("length should be non-negative"));
        for _ in 0..length {
            let element: Item = buf.get_decoded();
            elements.push(element);
        }
        Self { elements }
    }
}

#[derive(Debug, Default)]
pub struct CompactArray<Item> {
    pub elements: Vec<Item>,
}

// TODO: Add null array case
impl<T> From<Vec<T>> for CompactArray<T> {
    fn from(value: Vec<T>) -> Self {
        Self { elements: value }
    }
}

impl<Item: Encode> Encode for CompactArray<Item> {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        let length = u64::try_from(self.elements.len() + 1)
            .expect("Arrays should be smaller than u64::max in length");
        buf.put_encoded::<UnsignedVarint>(&length.into());
        for x in &self.elements {
            buf.put_encoded(x);
        }
    }
}

impl<Item: Decode> Decode for CompactArray<Item> {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        let length = buf
            .get_decoded::<UnsignedVarint>()
            .inner()
            .saturating_sub(1)
            .try_into()
            .expect("Compact array length should be less than usize::max");
        let mut elements = Vec::with_capacity(length);
        for _ in 0..length {
            elements.push(buf.get_decoded());
        }
        Self { elements }
    }
}

pub type Boolean = bool;
impl Encode for Boolean {
    fn encode<T: BufMut + ?Sized>(&self, buf: &mut T) {
        buf.put_u8(u8::from(*self));
    }
}
impl Decode for Boolean {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        buf.get_u8() != 0
    }
}

pub type Uuid = uuid::Uuid;
impl Encode for Uuid {
    fn encode<T: BufMut + ?Sized>(&self, buf: &mut T) {
        buf.put_slice(self.as_bytes());
    }
}
impl Decode for Uuid {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        let mut bytes = [0; 16];
        buf.copy_to_slice(&mut bytes);
        Self::from_bytes(bytes)
    }
}

/// Poor man's trait for numbers
/// Mainly created for use in generic bounds
pub trait Number:
    ops::Shl<Output = Self>
    + ops::Shr<Output = Self>
    + ops::BitXor<Output = Self>
    + ops::BitAnd<Output = Self>
    + ops::BitOr<Output = Self>
    + ops::Add<Output = Self>
    + ops::Sub<Output = Self>
    + From<u8>
    + TryInto<u8>
    + PartialOrd
    + Ord
    + Copy
    + Clone
    + PartialEq
    + Eq
    + fmt::Display
    + fmt::Debug
    + fmt::Binary
{
}

/// Signed numbers for numbers that can be negated
pub trait SignedNumber: Number + ops::Neg<Output = Self> {}

/// All signed numbers are numbers
impl<SN: SignedNumber> Number for SN {}

#[derive(Debug, Default)]
pub struct UnsignedVarnum<N: Number>(N);

impl Number for u64 {}
pub type UnsignedVarint = UnsignedVarnum<u64>;

impl<N: Number> From<N> for UnsignedVarnum<N> {
    fn from(value: N) -> Self {
        Self(value)
    }
}

impl<N: Number> UnsignedVarnum<N> {
    pub const fn inner(&self) -> N {
        self.0
    }
}

impl<N> Encode for UnsignedVarnum<N>
where
    N: Number,
    <N as TryInto<u8>>::Error: fmt::Debug,
{
    fn encode<T: BufMut + ?Sized>(&self, buf: &mut T) {
        //          10010110  // Original number: 150
        //  0000001  0010110  // Split into 7-bit segments
        // 00000001 10010110  // Add continuation bits
        // 10010110 00000001  // Encode as big endian (right to left)
        let mut n = self.0;
        loop {
            // We'll go from right to left as the final encoding has to be big-endian
            // Take right-most 7 bits
            let segment = n & 0b0111_1111.into();
            let segment: u8 = segment
                .try_into()
                .expect("One segment of Ntype should fit in u8");
            // Discard the right-most 7 bits
            n = n >> 7.into();
            // If there are no more bits left then we're done
            if n == 0.into() {
                // Encode the last segment
                buf.put_u8(segment);
                // And we're done
                break;
            }
            // Set the continuation bit as there are more segments
            let segment = segment | 0b1000_0000;
            println!("Encoding {:032b}, segment: {:08b}", self.0, segment);
            buf.put_u8(segment);
        }
    }
}

impl<N: Number> Decode for UnsignedVarnum<N> {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        let mut segments = vec![];
        loop {
            let segment = buf.get_u8();
            segments.push(segment);
            if segment & 0b1000_0000 == 0 {
                // No continuation bit
                break;
            }
        }

        // 10010110 00000001        // Segments.
        //  0010110  0000001        // Drop continuation bits.
        //  0000001  0010110        // Convert to big-endian.
        //    00000010010110        // Concatenate.
        //  128 + 16 + 4 + 2 = 150  // Interpret as an unsigned 64-bit integer.
        let mut n = N::from(0u8);
        for segment in segments.iter().rev() {
            n = (n << 7.into()) + N::from(*segment & 0b0111_1111);
        }
        Self(n)
    }
}

#[derive(Debug, Default)]
pub struct Varnum<SN: SignedNumber>(SN);

impl SignedNumber for i32 {}
pub type Varint = Varnum<i32>;
impl SignedNumber for i64 {}
pub type Varlong = Varnum<i64>;

impl<SN: SignedNumber> Varnum<SN> {
    pub const fn into_inner(self) -> SN {
        self.0
    }
}

impl<SN> Encode for Varnum<SN>
where
    SN: SignedNumber,
    <SN as TryInto<u8>>::Error: fmt::Debug,
{
    fn encode<T: BufMut + ?Sized>(&self, buf: &mut T) {
        let n = self.0;
        let bytes = u8::try_from(size_of::<SN>())
            .expect("NType should be a numeric type, with size less than or equal to 5");
        let shift: u8 = (bytes * 8) - 1;
        // n = -2        :   11111111 11111111 11111111 11111110
        // -----------------------------------------------------
        // n << 1        :   11111111 11111111 11111111 11111100
        // n >> 31       : ^ 11111111 11111111 11111111 11111111
        //                 -------------------------------------
        //                   00000000 00000000 00000000 00000011 = 3
        //------------------------------------------------------
        // n = 5        :    00000000 00000000 00000000 00000101
        // -----------------------------------------------------
        // n << 1        :   00000000 00000000 00000000 00001010
        // n >> 31       : ^ 00000000 00000000 00000000 00000000
        //                 -------------------------------------
        //                   00000000 00000000 00000000 00001010 = 10
        // Equivalent to 2n when n is positive, and 2n + 1 when n is negative,
        // as `n >> 31` is 0 (0x0000_0000) for positive and ~0 (0xFFFF_FFFF) for negative
        // and XORing with last bit set to 0 (via `n << 1`) results in adding 0 or 1 respectively
        let n_zig_zag = (n << 1u8.into()) ^ (n >> shift.into());

        if n_zig_zag.lt(&128.into()) {
            buf.put_u8(
                n_zig_zag
                    .try_into()
                    .expect("Varint should be smaller than u8::max / 2, probably time to refactor"),
            );
            return;
        }
        todo!("Implement varint encoding for multiple bytes");
    }
}

#[expect(
    clippy::shadow_unrelated,
    reason = "shadowing used for step-wise numeric operations"
)]
impl<SN: SignedNumber> Decode for Varnum<SN> {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        let mut segments = vec![];
        loop {
            let segment = buf.get_u8();
            segments.push(segment);
            if segment & 0b1000_0000 == 0 {
                // No continuation bit
                break;
            }
        }

        // 10010110 00000001        // Original inputs.
        //  0010110  0000001        // Drop continuation bits.
        //  0000001  0010110        // Convert to big-endian.
        //    00000010010110        // Concatenate.
        //  128 + 16 + 4 + 2 = 150  // Interpret as an unsigned 64-bit integer.
        let mut n = SN::from(0u8);
        for segment in segments.iter().rev() {
            n = (n << 7.into()) + SN::from(*segment & 0b0111_1111);
        }

        let n_zig_zag = n;
        let n = (n_zig_zag >> 1.into()) ^ -(n_zig_zag & 1.into());
        Self(n)
    }
}

// TODO: Make this actually TagBuffer, see https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=120722234#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-Serialization
pub type TagBuffer = i8;
// TODO: Is this beneficial? Should we use newtype instead of alias?
pub type ApiKey = i16;
pub type ApiVersion = i16;
pub type CorrelationId = i32;
pub type Int8 = i8;
pub type Int16 = i16;
pub type Int32 = i32;
pub type UInt32 = u32;
pub type Int64 = i64;

macro_rules! impl_encode_int {
    ($ty:ty) => {
        paste! {
            impl Encode for $ty {
                fn encode<T: BufMut + ?Sized>(&self, buf: &mut T) {
                    buf.[<put_$ty>](*self);
                }
            }
        }
    };
}

impl_encode_int!(u8);
impl_encode_int!(i8);
impl_encode_int!(i16);
impl_encode_int!(i32);
impl_encode_int!(u32);
impl_encode_int!(i64);

macro_rules! impl_decode_int {
    ($ty:ty) => {
        paste! {
            impl Decode for $ty {
                fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
                    buf.[<get_$ty>]()
                }
            }
        }
    };
}

impl_decode_int!(u8);
impl_decode_int!(i8);
impl_decode_int!(i16);
impl_decode_int!(i32);
impl_decode_int!(u32);
impl_decode_int!(i64);

// TODO: Separate this into another type?
pub type CompactBytes = CompactString;
pub type CompactNullableBytes = CompactNullableString;
pub type VarintBytes = VarintArray<u8>;

#[derive(Debug, Default)]
pub struct VarintSized<V>(Option<V>);

impl<V> VarintSized<V> {
    /// # Panics
    /// Panics if inner is not present
    pub fn into_inner(self) -> V {
        self.0.expect("inner should be present")
    }
}

impl<V> From<V> for VarintSized<V> {
    fn from(value: V) -> Self {
        Self(Some(value))
    }
}

impl<V: Encode> Encode for VarintSized<V> {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        let Some(value) = &self.0 else {
            buf.put_encoded(&Varnum(-1_i32));
            return;
        };
        // We need to write length first, so encode in a new buffer to get the length
        let mut bytes = BytesMut::new();
        bytes.put_encoded(value);
        let bytes = bytes.freeze();
        let len = bytes.len();
        buf.put_encoded(&Varnum(
            i32::try_from(len).expect("Varint len should fit in i32"),
        ));
        buf.put_encoded(&bytes);
    }
}

// INFO: Not providing this impl as it requires implementing Decode for values of V which are
// dependent on the size, which can be dangerous as such implementations will assume that the bytes
// being passed to them are theirs to consume. These could then be used without being wrapped by
// VarintSized, leading to issues. It is therefore better to directly implement Decode for concrete
// values of V
// impl<V: Decode> Decode for VarintSized<V> {
//     fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
//         let length: Varint = buf.get_decoded();
//         let length = length.0;
//         if length <= 0 {
//             return Self(None);
//         }
//         // Read length bytes as we need to decode only that many bytes
//         let mut length_buf =
//             buf.copy_to_bytes(usize::try_from(length).expect("varint len should fit in usize"));
//         let value = length_buf.get_decoded();
//         Self(Some(value))
//     }
// }

#[derive(Debug, Default)]
pub struct CompactString {
    pub value: Bytes,
}

impl Encode for CompactString {
    fn encode<B: BufMut + ?Sized>(&self, mut buf: &mut B) {
        // The length is encoded as 1 if the string is empty
        let length = u64::try_from(self.value.len() + 1)
            .expect("Compact strings should be smaller than u64::max in length");
        buf.put_encoded::<UnsignedVarint>(&length.into());
        buf.put_encoded(&self.value);
    }
}

impl Decode for CompactString {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        let length = buf
            .get_decoded::<UnsignedVarint>()
            .inner()
            .saturating_sub(1)
            .try_into()
            .expect("Compact string length should be less than usize::max");
        if length == 0 {
            return Self {
                value: Bytes::new(),
            };
        }
        let value = buf.copy_to_bytes(length);
        Self { value }
    }
}

impl From<&str> for CompactString {
    fn from(value: &str) -> Self {
        Self {
            value: Bytes::from(value.to_string()),
        }
    }
}

#[derive(Debug, Encode, SmartDefault)]
pub struct NullableString {
    #[default(-1_i16)]
    length: i16,
    value: Option<Bytes>,
}

impl NullableString {
    pub fn encoded_size(&self) -> usize {
        size_of_val(&self.length) + self.value.as_ref().map(Bytes::len).unwrap_or(0)
    }
}

impl Decode for NullableString {
    fn decode<B: Buf + ?Sized>(buf: &mut B) -> Self {
        let length = buf.get_i16();
        if length == -1 {
            return Self {
                length,
                value: None,
            };
        }
        let string_length = length.try_into().expect("length should be non-negative");
        let value = Some(buf.copy_to_bytes(string_length));
        Self { length, value }
    }
}

impl From<&str> for NullableString {
    fn from(value: &str) -> Self {
        Self {
            length: i16::try_from(value.len())
                .expect("Nullable string should be smaller than i16::max in length"),
            value: Some(Bytes::from(value.to_string())),
        }
    }
}

#[derive(Debug, Default)]
pub struct CompactNullableString {
    pub value: Option<Bytes>,
}

impl From<Bytes> for CompactNullableString {
    fn from(value: Bytes) -> Self {
        Self { value: Some(value) }
    }
}

impl Encode for CompactNullableString {
    fn encode<T: BufMut + ?Sized>(&self, mut buf: &mut T) {
        // The length is encoded as 0 if the string is empty, i.e. null
        let length = u64::try_from(self.value.as_ref().map_or(0, |x| x.len() + 1))
            .expect("Compact nullable strings should be smaller than u64::max in length");
        buf.put_encoded::<UnsignedVarint>(&length.into());
        buf.put_encoded(&self.value);
    }
}

impl Decode for CompactNullableString {
    fn decode<B: Buf + ?Sized>(mut buf: &mut B) -> Self {
        let length = buf
            .get_decoded::<UnsignedVarint>()
            .inner()
            .saturating_sub(1)
            .try_into()
            .expect("Compact nullable string length should be less than usize::max");
        if length == 0 {
            return Self { value: None };
        }
        let value = Some(buf.copy_to_bytes(length));
        Self { value }
    }
}

impl Encode for Option<Bytes> {
    fn encode<B: BufMut + ?Sized>(&self, mut buf: &mut B) {
        if let Some(x) = self {
            buf.put_encoded(x);
        }
    }
}

impl Encode for Bytes {
    fn encode<B: BufMut + ?Sized>(&self, buf: &mut B) {
        buf.put_slice(self);
    }
}
