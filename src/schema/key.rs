//! 稳定哈希与显式键编码；不能用默认 Hash 的平台相关输入冻结格式。
use crate::types::{Error, FormatId, HashDescriptor, KeyHash};

/// 编码是规范字节表示；相等的键必须具有相同编码和哈希。
/// 编码失败不得部分修改输出，输出长度必须恰好匹配 encoded_len。
/// 编码版本、算法或种子变化必须在读取记录前拒绝；不能静默重建索引掩盖变化。
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

    /// 检查持久材料声明的键语义；材料解析及恢复入口由后续任务接入。
    fn validate_identity(&self, format: FormatId, hash: &HashDescriptor) -> Result<(), Error> {
        if self.format_id() != format {
            return Err(Error::InvalidFormat("键编码版本不匹配"));
        }
        if &self.hash_descriptor() != hash {
            return Err(Error::InvalidFormat("键哈希算法或种子不匹配"));
        }
        Ok(())
    }
}
