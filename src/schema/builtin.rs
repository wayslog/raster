//! 内建键采用规范编码与稳定哈希；值布局依赖页所有者提供真实许可。
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

/// 原始字节键，包含空键和非 UTF-8 数据；不做 Unicode 归一化。
#[derive(Clone, Copy, Debug, Default)]
pub struct ByteKey;
/// 固定八字节小端整数键。
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

// 按 FNV-1a 64 位定义逐字节计算；不使用平台默认 Hasher。
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
        return Err(Error::Codec("键编码缓冲长度不匹配"));
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
            .map_err(|_| Error::Codec("整数键必须恰好八字节"))?;
        Ok(u64::from_le_bytes(bytes))
    }
}
/// 带长度前缀的通用字节槽，访问需要记录独占许可。
pub struct SerializedValue<C> {
    codec: C,
}
/// 原子整数布局，以逻辑数值编码，访问受页许可约束。
pub struct AtomicU64Value;

/// 原始字节值编码；空值与非 UTF-8 数据均合法。
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
/// 普通 u64 值的八字节小端编码。
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
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| Error::Codec("整数值必须恰好八字节"))?,
        ))
    }
}
impl<C: ValueCodec> SerializedValue<C> {
    pub fn new(codec: C) -> Self {
        Self { codec }
    }
    pub fn format_id(&self) -> FormatId {
        self.codec.format_id()
    }
    /// 活跃字节槽含八字节长度前缀；稳定编码不包含该进程内前缀。
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
    /// 与普通整数编码分离的布局身份；编码内容仍然是逻辑整数。
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
    fn 键长度边界无需分配巨大缓冲即可验证() {
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
