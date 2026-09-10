//! 内建键采用显式规范编码与固定版本哈希；值布局仍待实现。
use super::{KeyCodec, Schema, ValueLayout};
use crate::types::{Error, FormatId, HashDescriptor, KeyHash};
use std::marker::PhantomData;

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
/// 待实现通用字节槽；必须接入记录独占许可后才能实现 ValueLayout。
pub struct SerializedValue<C> {
    _codec: PhantomData<C>,
}
/// 待实现原子值布局；不能用普通字节转换代替原子初始化和稳定读取。
pub struct AtomicU64Value;

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
