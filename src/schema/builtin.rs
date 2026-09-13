//! Built-in keys use canonical encoding and stable hashing;Value layout relies on the page owner to provide real permission.
use super::value::{PreparedValue, ValueCodec};
use super::{KeyCodec, Schema, ValueLayout};
use crate::types::{Error, FormatId, HashDescriptor, KeyHash};

pub struct SchemaPair<K: KeyCodec, V: ValueLayout> {
    key: K,
    value: V,
}
impl<K: KeyCodec, V: ValueLayout> SchemaPair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}
impl<K: KeyCodec, V: ValueLayout> Schema for SchemaPair<K, V> {
    type Key = K;
    type Value = V;
    fn key_codec(&self) -> &K {
        &self.key
    }
    fn value_layout(&self) -> &V {
        &self.value
    }
}

/// raw byte key,Contains empty keys and non- UTF-8 data;Don't do it Unicode normalization.
#[derive(Clone, Copy, Debug, Default)]
pub struct ByteKey;
/// Fixed eight-byte little-endian integer keys.
#[derive(Clone, Copy, Debug, Default)]
pub struct U64Key;

const BYTE_FORMAT: FormatId = FormatId(*b"raster:bytes:v01");
const U64_FORMAT: FormatId = FormatId(*b"raster:u64le:v01");
const HASH_FORMAT: FormatId = FormatId(*b"raster:fnv1a64:1");
const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

fn hash_descriptor() -> HashDescriptor {
    HashDescriptor {
        algorithm: HASH_FORMAT,
        seed: OFFSET_BASIS.to_le_bytes().to_vec(),
    }
}

// press FNV-1a 64 Bit definition calculated byte by byte;Do not use platform default Hasher.
fn stable_hash(bytes: &[u8]) -> KeyHash {
    KeyHash(bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    }))
}

fn byte_length(bytes: &[u8]) -> Result<u32, Error> {
    checked_length(bytes.len())
}

fn checked_length(length: usize) -> Result<u32, Error> {
    u32::try_from(length).map_err(|_| Error::CapacityExceeded)
}

fn encode_exact(bytes: &[u8], output: &mut [u8]) -> Result<(), Error> {
    if bytes.len() != output.len() {
        return Err(Error::Codec("Key encoding buffer length mismatch"));
    }
    output.copy_from_slice(bytes);
    Ok(())
}

impl KeyCodec for ByteKey {
    type Key = [u8];
    type OwnedKey = Vec<u8>;
    fn format_id(&self) -> FormatId {
        BYTE_FORMAT
    }
    fn hash_descriptor(&self) -> HashDescriptor {
        hash_descriptor()
    }
    fn hash(&self, key: &[u8]) -> KeyHash {
        stable_hash(key)
    }
    fn encoded_len(&self, key: &[u8]) -> Result<u32, Error> {
        byte_length(key)
    }
    fn encode(&self, key: &[u8], output: &mut [u8]) -> Result<(), Error> {
        byte_length(key)?;
        encode_exact(key, output)
    }
    fn equals_encoded(&self, key: &[u8], encoded: &[u8]) -> Result<bool, Error> {
        byte_length(key)?;
        byte_length(encoded)?;
        Ok(key == encoded)
    }
    fn decode_owned(&self, encoded: &[u8]) -> Result<Vec<u8>, Error> {
        byte_length(encoded)?;
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(encoded.len())
            .map_err(|_| Error::OutOfMemory)?;
        owned.extend_from_slice(encoded);
        Ok(owned)
    }
}

impl KeyCodec for U64Key {
    type Key = u64;
    type OwnedKey = u64;
    fn format_id(&self) -> FormatId {
        U64_FORMAT
    }
    fn hash_descriptor(&self) -> HashDescriptor {
        hash_descriptor()
    }
    fn hash(&self, key: &u64) -> KeyHash {
        stable_hash(&key.to_le_bytes())
    }
    fn encoded_len(&self, _key: &u64) -> Result<u32, Error> {
        Ok(8)
    }
    fn encode(&self, key: &u64, output: &mut [u8]) -> Result<(), Error> {
        encode_exact(&key.to_le_bytes(), output)
    }
    fn equals_encoded(&self, key: &u64, encoded: &[u8]) -> Result<bool, Error> {
        Ok(*key == self.decode_owned(encoded)?)
    }
    fn decode_owned(&self, encoded: &[u8]) -> Result<u64, Error> {
        let bytes = encoded
            .try_into()
            .map_err(|_| Error::Codec("Integer keys must be exactly eight bytes"))?;
        Ok(u64::from_le_bytes(bytes))
    }
}
/// Generic byte slot with length prefix,Access requires record exclusive license.
pub struct SerializedValue<C> {
    codec: C,
}
/// Atomic integer layout,Encoded as a logical value,Access subject to page permission.
pub struct AtomicU64Value;

/// raw byte value encoding;Null value and not UTF-8 The data is legal.
#[derive(Clone, Copy, Debug, Default)]
pub struct ByteValueCodec;
impl ValueCodec for ByteValueCodec {
    type Value = Vec<u8>;
    fn format_id(&self) -> FormatId {
        FormatId(*b"raster:valbytes1")
    }
    fn encode(&self, value: &Vec<u8>) -> Result<Vec<u8>, Error> {
        self.decode(value)
    }
    fn encode_view<'a>(&self, value: &'a Vec<u8>) -> Result<std::borrow::Cow<'a, [u8]>, Error> {
        checked_length(value.len())?;
        Ok(std::borrow::Cow::Borrowed(value))
    }
    fn decode(&self, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        checked_length(bytes.len())?;
        let mut value = Vec::new();
        value
            .try_reserve_exact(bytes.len())
            .map_err(|_| Error::OutOfMemory)?;
        value.extend_from_slice(bytes);
        Ok(value)
    }
}
/// Ordinary u64 Eight-byte little-endian encoding of the value.
#[derive(Clone, Copy, Debug, Default)]
pub struct U64ValueCodec;
impl ValueCodec for U64ValueCodec {
    type Value = u64;
    fn format_id(&self) -> FormatId {
        FormatId(*b"raster:valu64le1")
    }
    fn encode(&self, value: &u64) -> Result<Vec<u8>, Error> {
        Ok(value.to_le_bytes().to_vec())
    }
    fn decode(&self, bytes: &[u8]) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
            Error::Codec("The integer value must be exactly eight bytes")
        })?))
    }
}
impl<C: ValueCodec> SerializedValue<C> {
    pub fn new(codec: C) -> Self {
        Self { codec }
    }
    pub fn format_id(&self) -> FormatId {
        self.codec.format_id()
    }
    /// Active byte slots have an eight-byte length prefix;Stable encodings do not contain this in-process prefix.
    pub fn prepare(&self, value: &C::Value) -> Result<PreparedValue, Error> {
        let bytes = self.codec.encode(value)?;
        let len = bytes.len().checked_add(8).ok_or(Error::CapacityExceeded)?;
        PreparedValue::new(bytes, len, 8)
    }
    pub fn decode_owned(&self, encoded: &[u8]) -> Result<C::Value, Error> {
        checked_length(encoded.len())?;
        self.codec.decode(encoded)
    }
}
impl AtomicU64Value {
    /// Layout identity separate from normal integer encoding;The encoded content is still a logical integer.
    pub fn format_id(&self) -> FormatId {
        FormatId(*b"raster:atomic641")
    }
    pub fn prepare(&self, value: u64) -> Result<PreparedValue, Error> {
        PreparedValue::new(
            value.to_le_bytes().to_vec(),
            std::mem::size_of::<std::sync::atomic::AtomicU64>(),
            std::mem::align_of::<std::sync::atomic::AtomicU64>(),
        )
    }
    pub fn decode_owned(&self, encoded: &[u8]) -> Result<u64, Error> {
        U64ValueCodec.decode(encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_length_bounds_can_be_verified_without_allocating_huge_buffers() {
        assert_eq!(checked_length(u32::MAX as usize).unwrap(), u32::MAX);
        if let Some(too_large) = (u32::MAX as usize).checked_add(1) {
            assert!(matches!(
                checked_length(too_large),
                Err(Error::CapacityExceeded)
            ));
        }
    }
}

mod layout;
pub use layout::SerializedUpdate;

#[cfg(test)]
mod owned_value_tests {
    use super::*;
    #[test]
    fn owned_value_decoding_is_consistent_with_slot_decoding_and_does_not_depend_on_the_input_life_cycle()
     {
        for value in [0u64, 1, u64::MAX] {
            let bytes = value.to_le_bytes();
            assert_eq!(
                ValueLayout::decode_owned(&AtomicU64Value, &bytes).unwrap(),
                value
            );
        }
        assert!(ValueLayout::decode_owned(&AtomicU64Value, &[0; 7]).is_err());
        for input in [vec![], vec![0, 255, 128], vec![7; 1500]] {
            let expected = input.clone();
            let layout = std::sync::Arc::new(SerializedValue::new(ByteValueCodec));
            let owned = ValueLayout::decode_owned(&*layout, &input).unwrap();
            let log = crate::log::HybridLog::new(
                crate::config::LogConfig {
                    page_bytes: 4096,
                    memory_pages: 2,
                    mutable_fraction: 0.5,
                },
                layout,
            )
            .unwrap();
            let temporary = log.decode_temporary(&input).unwrap();
            assert_eq!(temporary.read(|value| value).unwrap(), owned);
            drop(temporary);
            drop(log);
            drop(input);
            assert_eq!(owned, expected);
        }
    }
}

#[cfg(test)]
mod encoding_tests;
