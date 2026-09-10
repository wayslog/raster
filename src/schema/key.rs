//! 稳定哈希与显式键编码；不能用默认 Hash 的平台相关输入冻结格式。
use crate::types::{Error, FormatId, HashDescriptor, KeyHash};

pub trait KeyCodec: Send + Sync + 'static {
    type Key: ?Sized;
    type OwnedKey: 'static;
    fn format_id(&self) -> FormatId;
    fn hash_descriptor(&self) -> HashDescriptor;
    fn hash(&self, key: &Self::Key) -> KeyHash;
    fn encoded_len(&self, key: &Self::Key) -> Result<u32, Error>;
    fn encode(&self, key: &Self::Key, output: &mut [u8]) -> Result<(), Error>;
    fn equals_encoded(&self, key: &Self::Key, encoded: &[u8]) -> Result<bool, Error>;
    fn decode_owned(&self, encoded: &[u8]) -> Result<Self::OwnedKey, Error>;
}
